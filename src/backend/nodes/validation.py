"""
Result validation and cross-validation nodes.
"""

import logging
from pocketflow import Node

logger = logging.getLogger(__name__)

from backend.utils.data_source_manager import data_source_manager


class ResultValidator(Node):
    """
    Validate execution results against the original question and entity map.
    """

    def prep(self, shared):
        return {
            "exec_result": shared.get("exec_result"),
            "entities": shared.get("entities", []),
            "entity_map": shared.get("entity_map", {}),
            "question": shared["question"],
            "cross_references": shared.get("cross_references", {}),
        }

    def exec(self, prep_res):
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
            if entity_lower in result_str or any(part in result_str for part in entity_lower.split()):
                validation["entities_found"].append(entity)

                tables_with_data = entity_map.get(entity, {})
                completeness = len(tables_with_data)
                validation["data_completeness"][entity] = {
                    "tables_found": list(tables_with_data.keys()),
                    "completeness_score": min(completeness / 3, 1.0),
                }
            else:
                validation["entities_missing"].append(entity)
                validation["suggestions"].append(f"Re-search for {entity} with alternative name patterns")

        if isinstance(exec_result, dict):
            for entity in entities:
                if entity in exec_result:
                    entity_data = exec_result[entity]
                    if isinstance(entity_data, dict):
                        found_tables = entity_data.get("found_in_tables", [])
                        if len(found_tables) < 2:
                            validation["suggestions"].append(f"Limited data for {entity} - only in {found_tables}")

        return validation

    def post(self, shared, prep_res, exec_res):
        shared["validation_result"] = exec_res

        if exec_res["entities_missing"]:
            logger.warning(f"Validation: Missing data for {exec_res['entities_missing']}")
        else:
            logger.info(f"Validation: All {len(exec_res['entities_found'])} entities found in results")

        return "default"


class CrossValidator(Node):
    """
    Compare CSV and API execution results, flag discrepancies, and reconcile values.
    """

    def prep(self, shared):
        return {
            "csv_result": shared.get("csv_exec_result"),
            "api_result": shared.get("api_exec_result"),
            "entity_ids": shared.get("entity_ids", {}),
        }

    @staticmethod
    def _compare_scalars(csv_value, api_value):
        if csv_value is None or api_value is None:
            return None, None
        try:
            csv_float = float(csv_value)
            api_float = float(api_value)
            if api_float == 0:
                return 0.0, 0.0
            diff_pct = abs(csv_float - api_float) / abs(api_float)
            severity = "minor" if diff_pct < 0.02 else "moderate" if diff_pct < 0.05 else "major"
            return diff_pct, severity
        except (TypeError, ValueError):
            return None, None

    def exec(self, prep_res):
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
                preferred, _source = data_source_manager.reconcile_conflicts(csv_val, api_val, key)
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
            agreement_score = max(0.0, 1.0 - sum(item["diff_pct"] for item in discrepancies) / len(discrepancies))

        return {
            "agreement_score": agreement_score,
            "discrepancies": discrepancies,
            "reconciled": reconciled,
        }

    def post(self, shared, prep_res, exec_res):
        shared["cross_validation"] = exec_res
        if exec_res.get("reconciled") is not None:
            shared["exec_result"] = exec_res["reconciled"]
        logger.info(f"Cross validation completed. Agreement score: {exec_res.get('agreement_score')}")
        return "default"
