"""Result validation and cross-validation nodes."""

import logging

from pocketflow import Node

logger = logging.getLogger(__name__)

from backend.utils.data_source_manager import data_source_manager


class ResultValidator(Node):
    """Validate execution results against the original question and entity map."""

    def prep(self, shared):
        """
        Collect required execution context values from the shared context.

        Parameters:
            shared (dict): Shared execution context containing runtime values and metadata. Expected keys:
                - "exec_result": the raw execution result (any)
                - "entities": list of entity identifiers or names (defaults to [])
                - "entity_map": mapping with metadata about entities (defaults to {})
                - "question": the original user question (required)
                - "cross_references": auxiliary cross-reference data (defaults to {})

        Returns:
            dict: A dictionary with keys "exec_result", "entities", "entity_map", "question", and "cross_references" populated from `shared` (using defaults where noted).
        """
        return {
            "exec_result": shared.get("exec_result"),
            "entities": shared.get("entities", []),
            "entity_map": shared.get("entity_map", {}),
            "question": shared["question"],
            "cross_references": shared.get("cross_references", {}),
        }

    def exec(self, prep_res):
        """
        Builds a validation report for execution results against expected entities and an entity map.

        Parameters:
            prep_res (dict): Preparation dictionary with keys:
                - exec_result: The execution result (string, dict, or other) to validate.
                - entities (list[str]): Expected entity names to look for in the result.
                - entity_map (dict): Mapping from entity name to table information used to compute data completeness.

        Returns:
            dict: Validation report with the keys:
                - entities_found (list[str]): Entities detected in the execution result.
                - entities_missing (list[str]): Entities not detected.
                - data_completeness (dict): Per-entity completeness info mapping entity -> {
                    "tables_found": list[str],
                    "completeness_score": float  # score in [0.0, 1.0], computed from number of tables found
                  }
                - suggestions (list[str]): Actionable suggestions for missing or limited data.
        """
        exec_result = prep_res["exec_result"]
        entities = prep_res["entities"]
        entity_map = prep_res["entity_map"]

        validation = {
            "entities_found": [],
            "entities_missing": [],
            "data_completeness": {},
            "suggestions": [],
        }

        result_str = str(exec_result).lower() if exec_result else ""

        for entity in entities:
            entity_lower = entity.lower()
            if entity_lower in result_str or any(
                part in result_str for part in entity_lower.split()
            ):
                validation["entities_found"].append(entity)

                tables_with_data = entity_map.get(entity, {})
                completeness = len(tables_with_data)
                validation["data_completeness"][entity] = {
                    "tables_found": list(tables_with_data.keys()),
                    "completeness_score": min(completeness / 3, 1.0),
                }
            else:
                validation["entities_missing"].append(entity)
                validation["suggestions"].append(
                    f"Re-search for {entity} with alternative name patterns"
                )

        if isinstance(exec_result, dict):
            for entity in entities:
                if entity in exec_result:
                    entity_data = exec_result[entity]
                    if isinstance(entity_data, dict):
                        found_tables = entity_data.get("found_in_tables", [])
                        if len(found_tables) < 2:
                            validation["suggestions"].append(
                                f"Limited data for {entity} - only in {found_tables}"
                            )

        return validation

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Store the validation result in the shared context and print a brief status message.

        This function saves `exec_res` into `shared["validation_result"]`. It prints a message listing any missing entities when `exec_res["entities_missing"]` is non-empty, otherwise it prints that all found entities are present.

        Parameters:
            shared (dict): Execution-wide context where results are stored.
            prep_res (dict): Preparation output (not used by this post hook).
            exec_res (dict): Validation result containing at least the keys:
                - `entities_missing` (list): Entities not found.
                - `entities_found` (list): Entities found.

        Returns:
            str: The next node identifier, `"default"`.
        """
        shared["validation_result"] = exec_res

        if exec_res["entities_missing"]:
            logger.warning(
                f"Validation: Missing data for {exec_res['entities_missing']}"
            )
        else:
            logger.info(
                f"Validation: All {len(exec_res['entities_found'])} entities found in results"
            )

        return "default"


class CrossValidator(Node):
    """Compare CSV and API execution results, flag discrepancies, and reconcile values."""

    def prep(self, shared):
        """
        Extracts CSV, API execution results and entity IDs from the shared context.

        Parameters:
            shared (dict): Shared execution context containing optional keys "csv_exec_result",
                "api_exec_result", and "entity_ids".

        Returns:
            dict: A mapping with keys:
                - "csv_result": value of shared["csv_exec_result"] or None if missing.
                - "api_result": value of shared["api_exec_result"] or None if missing.
                - "entity_ids": value of shared["entity_ids"] or an empty dict if missing.
        """
        return {
            "csv_result": shared.get("csv_exec_result"),
            "api_result": shared.get("api_exec_result"),
            "entity_ids": shared.get("entity_ids", {}),
        }

    @staticmethod
    def _compare_scalars(csv_value, api_value):
        """
        Compute the relative difference between two scalar values and classify its severity.

        If either input is `None` or cannot be converted to a number, returns `(None, None)`. If `api_value` is zero, returns `(0.0, 0.0)` to indicate no computable relative difference.

        Parameters:
            csv_value: The value from the CSV source (numeric or numeric string).
            api_value: The value from the API source (numeric or numeric string).

        Returns:
            A tuple `(diff_pct, severity)` where `diff_pct` is the relative difference `abs(csv - api) / abs(api)` and `severity` is one of `minor`, `moderate`, or `major` based on thresholds (<0.02, <0.05, otherwise). Returns `(None, None)` when inputs are missing or non-numeric.
        """
        if csv_value is None or api_value is None:
            return None, None
        try:
            csv_float = float(csv_value)
            api_float = float(api_value)
            if api_float == 0:
                return 0.0, 0.0
            diff_pct = abs(csv_float - api_float) / abs(api_float)
            severity = (
                "minor"
                if diff_pct < 0.02
                else "moderate" if diff_pct < 0.05 else "major"
            )
            return diff_pct, severity
        except (TypeError, ValueError):
            return None, None

    def exec(self, prep_res):
        """
        Compare CSV and API execution results, record numeric discrepancies, and produce a reconciled value map.

        Parameters:
            prep_res (dict): Input preparation dictionary containing:
                - "csv_result": the result produced from CSV parsing (dict or other).
                - "api_result": the result produced from the API source (dict or other).

        Returns:
            dict: A report with:
                - "agreement_score" (float): Aggregate agreement score between sources (1.0 = full agreement, lower values indicate larger average relative differences).
                - "discrepancies" (list): List of discrepancy records; each record is a dict with keys:
                    - "field": field/key name compared.
                    - "csv": value from the CSV result.
                    - "api": value from the API result.
                    - "diff_pct": relative difference as a decimal (e.g., 0.02 for 2%).
                    - "severity": severity category string ('minor', 'moderate', 'major').
                - "reconciled" (dict or other): Reconciled result map produced by resolving conflicts between CSV and API values (or the non-null result if inputs are not both dicts).
        """
        csv_result = prep_res["csv_result"]
        api_result = prep_res["api_result"]
        discrepancies = []
        reconciled = {}

        if isinstance(csv_result, dict) and isinstance(api_result, dict):
            keys = set(csv_result.keys()) | set(api_result.keys())
            for key in keys:
                csv_val = csv_result.get(key)
                api_val = api_result.get(key)
                diff_pct, severity = self._compare_scalars(csv_val, api_val)
                preferred, _source = data_source_manager.reconcile_conflicts(
                    csv_val, api_val, key
                )
                reconciled[key] = preferred
                if diff_pct is not None and severity:
                    discrepancies.append(
                        {
                            "field": key,
                            "csv": csv_val,
                            "api": api_val,
                            "diff_pct": diff_pct,
                            "severity": severity,
                        }
                    )
        else:
            reconciled = csv_result or api_result

        agreement_score = 1.0
        if discrepancies:
            agreement_score = max(
                0.0,
                1.0
                - sum(item["diff_pct"] for item in discrepancies) / len(discrepancies),
            )

        return {
            "agreement_score": agreement_score,
            "discrepancies": discrepancies,
            "reconciled": reconciled,
        }

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Store cross-validation results into the shared context and update the executable result when available.

        Saves exec_res under the key "cross_validation" in shared. If exec_res contains a "reconciled" entry, sets shared["exec_result"] to that reconciled value. Prints the agreement score from exec_res.

        Parameters:
            shared (dict): Mutable shared execution context where results are stored.
            prep_res (dict): Preparation-phase data (not used by this method).
            exec_res (dict): Cross-validation output; expected to contain keys like "reconciled" and "agreement_score".

        Returns:
            str: The string "default".
        """
        shared["cross_validation"] = exec_res
        if exec_res.get("reconciled") is not None:
            shared["exec_result"] = exec_res["reconciled"]
        logger.info(
            f"Cross validation completed. Agreement score: {exec_res.get('agreement_score')}"
        )
        return "default"
