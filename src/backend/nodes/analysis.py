"""Analysis, visualization, and response synthesis nodes."""

import json
import logging
import os
import time

import matplotlib
import pandas as pd
from pocketflow import Node


logger = logging.getLogger(__name__)

from backend.config import (
    ANALYSIS_TRUNCATION,
    CHART_HISTORY_LIMIT,
    CHART_ROW_LIMIT,
    EXEC_RESULT_TRUNCATION,
    RAW_RESULT_TRUNCATION,
)
from backend.utils.call_llm import call_llm
from backend.utils.knowledge_store import knowledge_store


class DeepAnalyzer(Node):
    """Perform deeper LLM-backed analysis on execution results."""

    def prep(self, shared):
        """
        Prepare payload for DeepAnalyzer, assembling execution results and related context from the shared state.

        Parameters:
            shared (dict): Shared runtime context containing keys like "exec_result", "csv_exec_result", "api_exec_result", "question", "entity_map", "entities", and "cross_validation".

        Returns:
            dict: A prepared dictionary with keys:
                - "exec_result": primary execution result from `shared["exec_result"]` if present, otherwise a composite with "csv" and "api" results.
                - "question": the user's question from `shared["question"]`.
                - "entity_map": mapping of entity identifiers to metadata (defaults to an empty dict).
                - "entities": list of entities relevant to the query (defaults to an empty list).
                - "cross_validation": cross-validation data or metadata (defaults to an empty dict).
        """
        return {
            "exec_result": shared.get("exec_result")
            or {
                "csv": shared.get("csv_exec_result"),
                "api": shared.get("api_exec_result"),
            },
            "question": shared["question"],
            "entity_map": shared.get("entity_map", {}),
            "entities": shared.get("entities", []),
            "cross_validation": shared.get("cross_validation", {}),
        }

    def _check_data_completeness(self, exec_result, entities):
        """
        Check which requested entities are missing or have incomplete data in the execution result.

        Inspects exec_result (when it is a dict) for each entity name (case-insensitive) by looking at keys and stringified values. Treats None, empty dicts, and empty lists as missing/incomplete. Returns a list of entity names that were not found or whose data appears incomplete, and a parallel list of human-readable warning messages describing each problem.

        Parameters:
            exec_result (dict | any): The execution result to search; only dicts are inspected for entity data.
            entities (Iterable[str]): Names of entities to verify presence/completeness for.

        Returns:
            tuple:
                - missing_entities (list[str]): Entities not found or with incomplete/empty data.
                - data_warnings (list[str]): Warning messages corresponding to each missing or incomplete entity.
        """
        missing_entities = []
        data_warnings = []

        if isinstance(exec_result, dict):
            for entity in entities:
                entity_lower = entity.lower()
                found = False
                for key, value in exec_result.items():
                    if entity_lower in str(key).lower():
                        if isinstance(value, dict):
                            if not value or all(v is None or v in ({}, []) for v in value.values()):
                                missing_entities.append(entity)
                                data_warnings.append(f"Data for '{entity}' appears incomplete or empty")
                            else:
                                found = True
                        elif value is not None and value not in ({}, []):
                            found = True
                if not found and entity not in missing_entities:
                    for key, value in exec_result.items():
                        if entity_lower in str(value).lower():
                            found = True
                            break
                    if not found:
                        missing_entities.append(entity)
                        data_warnings.append(f"Could not find data for '{entity}' in results")

        return missing_entities, data_warnings

    def exec(self, prep_res):
        """
        Generate a structured deep analysis of execution results using an LLM and return the analysis augmented with data-quality metadata.

        Parameters:
            prep_res (dict): Preparation result containing:
                - "exec_result": the raw execution result to analyze (any type)
                - "question": the user's original question (str)
                - "entities": list of entity names requested (list[str])
                - "cross_validation": cross-validation summary to include in the prompt (any serializable)

        Returns:
            dict | None: A deep analysis structure when exec_result is present, otherwise `None`. The returned dict contains at least the keys:
                - "key_stats": dict of important statistics derived from available data
                - "comparison": comparison summary or `None` if insufficient data
                - "insights": list of insight strings based on actual data
                - "data_gaps": list of entities or data points that were not found
                - "narrative_points": list of points intended for inclusion in the final response
                - "_missing_entities": list of entities identified as missing (added metadata)
                - "_data_warnings": list of data-quality warnings (added metadata)
        """
        exec_result = prep_res["exec_result"]
        question = prep_res["question"]
        entities = prep_res["entities"]
        cross_validation = prep_res["cross_validation"]

        if exec_result is None:
            return None

        missing_entities, data_warnings = self._check_data_completeness(exec_result, entities)

        result_str = str(exec_result)
        if len(result_str) > RAW_RESULT_TRUNCATION:
            result_str = result_str[:RAW_RESULT_TRUNCATION] + "... [truncated]"

        entities_str = ", ".join(entities) if entities else "the data"

        warning_note = ""
        if data_warnings:
            warning_note = "\n\nDATA QUALITY WARNING:\n" + "\n".join(f"- {warn}" for warn in data_warnings)
            warning_note += (
                "\nIMPORTANT: Only analyze data that is actually present. "
                "Do NOT make up statistics for missing entities."
            )

        prompt = f"""You are a sports data analyst. Analyze the following data results and provide insights.

ORIGINAL QUESTION: <user_question>{question}</user_question>

ENTITIES BEING ANALYZED: {entities_str}
{warning_note}
RAW DATA RESULTS:
<raw_data>
{result_str}
</raw_data>
CROSS VALIDATION SUMMARY:
{json.dumps(cross_validation, indent=2, default=str)}

Provide analysis based ONLY on data that is actually present:
1. KEY STATISTICS: Summarize the most important numbers (only from actual data)
2. COMPARISONS: Compare only entities with complete data
3. INSIGHTS: What insights can we draw from the available data
4. DATA GAPS: Clearly note which entities have incomplete data

CRITICAL: Do NOT fabricate or hallucinate statistics. If data is missing, say so clearly.

Return your analysis as a structured JSON object with these keys:
- \"key_stats\": dict of important statistics (from actual data only)
- \"comparison\": summary of comparisons (null if insufficient data)
- \"insights\": list of insights (based on actual data)
- \"data_gaps\": list of entities or data points that were not found
- \"narrative_points\": list of points to include in final response

Return ONLY valid JSON."""

        try:
            analysis_response = call_llm(prompt)
            analysis_response = (analysis_response or "").strip()
            if analysis_response.startswith("```"):
                analysis_response = analysis_response.split("```")[1]
                analysis_response = analysis_response.removeprefix("json")
            deep_analysis = json.loads(analysis_response)
            deep_analysis["_missing_entities"] = missing_entities
            deep_analysis["_data_warnings"] = data_warnings
        except json.JSONDecodeError as exc:
            logger.exception(f"Failed to parse deep analysis JSON: {exc}")
            deep_analysis = {
                "key_stats": {"raw_result": str(exec_result)[:500]},
                "comparison": None,
                "insights": ["Analysis completed with available data"],
                "data_gaps": missing_entities,
                "_missing_entities": missing_entities,
                "_data_warnings": data_warnings,
            }
        except Exception as exc:
            logger.exception(f"Unexpected error in deep analysis: {exc}")
            deep_analysis = {
                "key_stats": {"raw_result": str(exec_result)[:500]},
                "comparison": None,
                "insights": ["Analysis completed with available data"],
                "data_gaps": missing_entities,
                "_missing_entities": missing_entities,
                "_data_warnings": data_warnings,
            }

        return deep_analysis

    def exec_fallback(self, prep_res, exc) -> None:
        """
        Handle execution failures for DeepAnalyzer by reporting the error and returning None.

        Parameters:
            prep_res: The prepared input that was passed to exec; may be used for diagnostics.
            exc: The exception instance that caused the failure; its message is reported.
        """
        logger.error(f"DeepAnalyzer failed: {exc}")

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Store the deep analysis result in the shared pipeline state and log completion status.

        Parameters:
            shared (dict): Mutable pipeline state; will be updated with key "deep_analysis" set to exec_res.
            prep_res: Prepared input for execution (unused by this post step).
            exec_res (dict | None): Result of deep analysis; if present and contains the "_data_warnings" key, warnings are printed.

        Returns:
            str: The next pipeline step identifier, always `"default"`.
        """
        shared["deep_analysis"] = exec_res
        if exec_res:
            if exec_res.get("_data_warnings"):
                logger.warning(f"Deep analysis completed with warnings: {exec_res['_data_warnings']}")
            else:
                logger.info("Deep analysis completed.")
        return "default"


class Visualizer(Node):
    """Generate simple bar chart visualizations from DataFrame outputs."""

    def prep(self, shared):
        """
        Extracts the execution result from the shared pipeline state for visualization.

        Parameters:
            shared (dict): Shared pipeline state containing prior node outputs.

        Returns:
            exec_result: The value stored under "exec_result" in `shared`, or `None` if that key is absent.
        """
        return shared.get("exec_result")

    def exec(self, prep_res):
        """
        Create a simple bar chart from the first numeric column of a DataFrame and save it to the assets directory.

        Parameters:
            prep_res (pandas.DataFrame | None): DataFrame to visualize; may be None or non-DataFrame in which case no chart is produced.

        Returns:
            str | None: File path to the generated PNG chart (assets/chart_<timestamp>.png) if a chart was created, `None` if no chart could be produced (e.g., prep_res is None, not a DataFrame, or contains no numeric columns).

        Notes:
            - Ensures the "assets" directory exists and prunes older chart files beyond CHART_HISTORY_LIMIT.
            - The chart uses up to CHART_ROW_LIMIT rows and plots the first numeric column as a bar chart.
        """
        if prep_res is None:
            return None
        if isinstance(prep_res, pd.DataFrame):
            numeric_cols = [col for col in prep_res.columns if pd.api.types.is_numeric_dtype(prep_res[col])]
            if not numeric_cols:
                return None

            matplotlib.use("Agg")
            import matplotlib.pyplot as plt

            output_dir = "assets"
            os.makedirs(output_dir, exist_ok=True)

            try:
                chart_files = sorted(
                    [file for file in os.listdir(output_dir) if file.startswith("chart_")],
                    key=lambda name: os.path.getmtime(os.path.join(output_dir, name)),
                )
                for old_file in chart_files[:-CHART_HISTORY_LIMIT]:
                    os.remove(os.path.join(output_dir, old_file))
            except OSError:
                pass

            timestamp = int(time.time())
            output_path = os.path.join(output_dir, f"chart_{timestamp}.png")

            plot_df = prep_res[numeric_cols].head(CHART_ROW_LIMIT)
            plt.figure(figsize=(8, 4))
            plot_df[numeric_cols[0]].plot(kind="bar")
            plt.title(f"Top {CHART_ROW_LIMIT} rows by {numeric_cols[0]}")
            plt.xlabel("Row")
            plt.ylabel(numeric_cols[0])
            plt.tight_layout()
            plt.savefig(output_path)
            plt.close()
            return output_path
        return None

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Store the generated chart path in the shared state and continue the default flow.

        Parameters:
            exec_res (str | None): File path to the generated chart image, or None if no chart was produced.

        Returns:
            "default": Signal indicating the default post-execution transition.
        """
        shared["chart_path"] = exec_res
        return "default"


class ResponseSynthesizer(Node):
    """Compose the final response using execution results and deep analysis output."""

    def prep(self, shared):
        """
        Assembles a response-synthesis payload from the shared runtime state.

        Parameters:
            shared (dict): Runtime context containing keys used to build the payload. Expected keys:
                - "exec_result": raw execution result (optional)
                - "deep_analysis": structured analysis produced earlier (optional)
                - "question": original user question (required)
                - "entities": list of requested entities (optional, defaults to [])
                - "entity_map": mapping of entity identifiers to metadata (optional, defaults to {})
                - "cross_validation": cross-validation summary (optional, defaults to {})
                - "data_sources": metadata about data sources (optional, defaults to {})

        Returns:
            dict: Payload with the following keys:
                - "exec_result": value of shared["exec_result"] or None
                - "deep_analysis": value of shared["deep_analysis"] or None
                - "question": shared["question"]
                - "entities": list of entities (defaults to [])
                - "entity_map": mapping of entities (defaults to {})
                - "cross_validation": cross-validation info (defaults to {})
                - "data_sources": data source metadata (defaults to {})
                - "from_error": `True` if "exec_result" is missing from shared, `False` otherwise
        """
        return {
            "exec_result": shared.get("exec_result"),
            "deep_analysis": shared.get("deep_analysis"),
            "question": shared["question"],
            "entities": shared.get("entities", []),
            "entity_map": shared.get("entity_map", {}),
            "cross_validation": shared.get("cross_validation", {}),
            "data_sources": shared.get("data_sources", {}),
            "from_error": "exec_result" not in shared,
        }

    def exec(self, prep_res):
        """
        Compose the final user-facing response by combining raw execution results, deep analysis, and data-quality information, invoking the LLM to produce a structured answer and updating the knowledge store with any discovered entity mappings.

        Parameters:
            prep_res (dict): Prepared payload containing at least:
                - "from_error" (bool): if True, indicates upstream failure and causes this call to return None.
                - "exec_result": raw execution result (e.g., DataFrame or text).
                - "deep_analysis" (dict|None): analysis output that may include private keys `_missing_entities` and `_data_warnings`.
                - "question" (str): the user's original question.
                - "entities" (list[str]): entities referenced by the question.
                - "entity_map" (dict): mappings used to update the knowledge store.
                - Optional "cross_validation" and "data_sources" for inclusion in the prompt.

        Returns:
            str | None: A formatted textual response generated by the LLM, or None when prep_res indicates an upstream error.

        Side effects:
            - May add entity mappings and a successful query pattern to the global knowledge_store for entities found in the data.
            - Truncates long raw results and analysis when building the prompt to the configured limits.
            - Ensures the generated response explicitly notes missing or incomplete data as indicated by deep_analysis.
        """
        if prep_res["from_error"]:
            return None

        exec_result = prep_res["exec_result"]
        deep_analysis = prep_res["deep_analysis"]
        question = prep_res["question"]
        entities = prep_res["entities"]
        cross_validation = prep_res.get("cross_validation", {})
        data_sources = prep_res.get("data_sources", {})

        result_str = str(exec_result)
        if len(result_str) > EXEC_RESULT_TRUNCATION:
            result_str = result_str[:EXEC_RESULT_TRUNCATION] + "... [truncated]"

        missing_entities = deep_analysis.get("_missing_entities", []) if deep_analysis else []
        data_warnings = deep_analysis.get("_data_warnings", []) if deep_analysis else []

        safe_analysis = {key: value for key, value in (deep_analysis or {}).items() if not key.startswith("_")}
        analysis_str = json.dumps(safe_analysis, indent=2, default=str) if safe_analysis else "No deep analysis available"
        if len(analysis_str) > ANALYSIS_TRUNCATION:
            analysis_str = analysis_str[:ANALYSIS_TRUNCATION] + "... [truncated]"

        entities_str = " and ".join(entities) if entities else "the requested data"

        data_quality_note = ""
        if missing_entities or data_warnings:
            data_quality_note = f"""

DATA QUALITY NOTICE:
The following entities had incomplete or missing data: {', '.join(missing_entities) if missing_entities else 'None identified'}
Warnings: {'; '.join(data_warnings) if data_warnings else 'None'}

CRITICAL INSTRUCTION: Do NOT fabricate or hallucinate any statistics or facts for entities with missing data.
If data is incomplete, clearly state what data was found and what was not available.
Be honest about the limitations of the analysis."""

        prompt = f"""You are a sports analyst writing a response to a user's question.

QUESTION: <user_question>{question}</user_question>

ENTITIES: {entities_str}
{data_quality_note}
RAW DATA (from actual CSV analysis):
<raw_data>
{result_str}
</raw_data>

ANALYSIS:
<analysis>
{analysis_str}
</analysis>
CROSS VALIDATION:
{json.dumps(cross_validation, indent=2, default=str)}
DATA SOURCES:
{json.dumps(data_sources, indent=2, default=str)}

Write a well-structured response that:
1. Directly addresses the user's question based on ACTUAL DATA ONLY
2. Provides key statistics from the data that was found
3. CLEARLY STATES if data for any entity was not found or incomplete
4. Uses clear sections/headers for readability
5. Does NOT make up statistics - only report what was actually found

If data for some entities is missing, explicitly state:
\"Note: Complete data for [entity] was not found in the available datasets.\"

Write in a professional tone. Use markdown formatting.
Be honest about data limitations - do not fabricate facts."""

        response = call_llm(prompt)
        if not response:
            response = "Unable to generate a response. Please try again."

        if entities:
            for entity in entities:
                if entity not in missing_entities:
                    for table, cols in prep_res["entity_map"].get(entity, {}).items():
                        knowledge_store.add_entity_mapping(entity, table, cols)
            if not missing_entities:
                knowledge_store.add_successful_pattern(
                    "comparison" if len(entities) > 1 else "lookup",
                    question[:100],
                )

        return response

    def exec_fallback(self, prep_res, exc) -> str:
        """
        Handle failures during ResponseSynthesizer execution and provide a user-facing apology message.

        Parameters:
            prep_res: The prepared input that was being processed when the error occurred.
            exc (Exception): The exception that triggered the fallback.

        Returns:
            A user-facing apology string indicating a system error and inability to generate a response.
        """
        logger.error(f"ResponseSynthesizer failed: {exc}")
        return "I apologize, but I am unable to generate a response at this time due to a system error."

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Store the final response in shared state, print a formatted "FINAL RESPONSE" block, and return the default next-step tag.

        Parameters:
            shared (dict): Shared pipeline state where the final text will be stored under the "final_text" key.
            prep_res: Preparation result (not used by this post step).
            exec_res: The execution result to store and display; if falsy, existing shared["final_text"] is left unchanged.

        Returns:
            str: The next-step tag "default".
        """
        if exec_res:
            shared["final_text"] = exec_res
        logger.info(f"\n{'='*60}")
        logger.info("FINAL RESPONSE:")
        logger.info("=" * 60)
        logger.info(shared.get("final_text", "No answer"))
        return "default"
