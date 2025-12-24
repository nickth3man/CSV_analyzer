"""
Analysis, visualization, and response synthesis nodes.
"""

import json
import os
import time

import matplotlib
import pandas as pd
from pocketflow import Node

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
        missing_entities = []
        data_warnings = []

        if isinstance(exec_result, dict):
            for entity in entities:
                entity_lower = entity.lower()
                found = False
                for key, value in exec_result.items():
                    if entity_lower in str(key).lower():
                        if isinstance(value, dict):
                            if not value or all(v is None or v == {} or v == [] for v in value.values()):
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
                if analysis_response.startswith("json"):
                    analysis_response = analysis_response[4:]
            deep_analysis = json.loads(analysis_response)
            deep_analysis["_missing_entities"] = missing_entities
            deep_analysis["_data_warnings"] = data_warnings
        except json.JSONDecodeError as exc:
            print(f"Failed to parse deep analysis JSON: {exc}")
            deep_analysis = {
                "key_stats": {"raw_result": str(exec_result)[:500]},
                "comparison": None,
                "insights": ["Analysis completed with available data"],
                "data_gaps": missing_entities,
                "_missing_entities": missing_entities,
                "_data_warnings": data_warnings,
            }
        except Exception as exc:  # noqa: BLE001
            print(f"Unexpected error in deep analysis: {exc}")
            deep_analysis = {
                "key_stats": {"raw_result": str(exec_result)[:500]},
                "comparison": None,
                "insights": ["Analysis completed with available data"],
                "data_gaps": missing_entities,
                "_missing_entities": missing_entities,
                "_data_warnings": data_warnings,
            }

        return deep_analysis

    def exec_fallback(self, prep_res, exc):
        print(f"DeepAnalyzer failed: {exc}")
        return None

    def post(self, shared, prep_res, exec_res):
        shared["deep_analysis"] = exec_res
        if exec_res:
            if exec_res.get("_data_warnings"):
                print(f"Deep analysis completed with warnings: {exec_res['_data_warnings']}")
            else:
                print("Deep analysis completed.")
        return "default"


class Visualizer(Node):
    """Generate simple bar chart visualizations from dataframe outputs."""

    def prep(self, shared):
        return shared.get("exec_result")

    def exec(self, prep_res):
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
            except (OSError, IOError):
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

    def post(self, shared, prep_res, exec_res):
        shared["chart_path"] = exec_res
        return "default"


class ResponseSynthesizer(Node):
    """Compose the final response using execution results and deep analysis output."""

    def prep(self, shared):
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

    def exec_fallback(self, prep_res, exc):
        print(f"ResponseSynthesizer failed: {exc}")
        return "I apologize, but I am unable to generate a response at this time due to a system error."

    def post(self, shared, prep_res, exec_res):
        if exec_res:
            shared["final_text"] = exec_res
        print(f"\n{'='*60}")
        print("FINAL RESPONSE:")
        print("=" * 60)
        print(shared.get("final_text", "No answer"))
        return "default"
