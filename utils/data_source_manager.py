from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


class DataSourceManager:
    """Coordinates CSV + NBA API dataframes and resolves conflicts."""

    DISCREPANCY_THRESHOLD = 0.05

    def detect_query_entities(self, question: str) -> List[str]:
        """Lightweight entity detection to inform API endpoint choices."""
        if not question:
            return []
        candidates = re.findall(r"[A-Z][a-z]+(?:\s[A-Z][a-z]+)*", question)
        seen: List[str] = []
        for candidate in candidates:
            if candidate not in seen:
                seen.append(candidate)
        return seen

    def determine_api_endpoints(self, entities: List[str], question: str) -> List[Dict[str, Any]]:
        """Map entities + intent to candidate endpoints."""
        endpoints: List[Dict[str, Any]] = []
        q_lower = question.lower()

        def add_endpoint(name: str, params: Optional[Dict[str, Any]] = None):
            endpoints.append({"name": name, "params": params or {}})

        if "score" in q_lower or "today" in q_lower or "live" in q_lower:
            add_endpoint("scoreboard", {})

        if any(k in q_lower for k in ["compare", "versus", "vs", "better", "greater"]):
            for ent in entities:
                add_endpoint("player_career", {"entity": ent})
                add_endpoint("league_leaders", {"entity": ent})

        if "lineup" in q_lower or "roster" in q_lower:
            for ent in entities:
                add_endpoint("common_team_roster", {"entity": ent})

        if "game log" in q_lower or "recent games" in q_lower:
            for ent in entities:
                add_endpoint("player_game_log", {"entity": ent})

        if not endpoints:
            for ent in entities:
                add_endpoint("player_career", {"entity": ent})

        return endpoints

    def merge_data_sources(
        self, csv_data: Dict[str, pd.DataFrame], api_data: Dict[str, pd.DataFrame]
    ) -> Tuple[Dict[str, pd.DataFrame], List[Dict[str, Any]], Dict[str, str]]:
        """Merge CSV + API dataframes with source tracking and discrepancies."""
        merged: Dict[str, pd.DataFrame] = {}
        discrepancies: List[Dict[str, Any]] = []
        sources: Dict[str, str] = {}

        csv_data = csv_data or {}
        api_data = api_data or {}

        for name, df in csv_data.items():
            merged[name] = df.copy()
            merged[name]["_source"] = "csv"
            sources[name] = "csv"

        for name, df in api_data.items():
            if name in merged:
                merged_df, table_discrepancies = self._merge_table(merged[name], df, name)
                merged[name] = merged_df
                discrepancies.extend(table_discrepancies)
                sources[name] = "merged"
            else:
                merged[name] = df.copy()
                merged[name]["_source"] = "api"
                sources[name] = "api"

        return merged, discrepancies, sources

    def _merge_table(
        self, csv_df: pd.DataFrame, api_df: pd.DataFrame, table_name: str
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Merge a single table and record discrepancies."""
        discrepancies: List[Dict[str, Any]] = []
        working_api_df = api_df.copy()
        working_csv_df = csv_df.copy()

        if "_source" not in working_api_df.columns:
            working_api_df["_source"] = "api"
        if "_source" not in working_csv_df.columns:
            working_csv_df["_source"] = "csv"

        common_keys = [
            key
            for key in ["PLAYER_ID", "PERSON_ID", "TEAM_ID"]
            if key in working_api_df.columns and key in working_csv_df.columns
        ]
        if common_keys:
            key = common_keys[0]
            merged = working_csv_df.merge(
                working_api_df, on=key, how="outer", suffixes=("_csv", "_api")
            )
            numeric_cols = [c for c in merged.columns if pd.api.types.is_numeric_dtype(merged[c])]
            for col in numeric_cols:
                if col.endswith("_csv"):
                    base = col[:-4]
                    api_col = f"{base}_api"
                    if api_col in merged.columns:
                        merged[base] = merged[api_col].combine_first(merged[col])
                        diff = (merged[col] - merged[api_col]).abs()
                        denom = merged[api_col].replace(0, pd.NA)
                        diff_pct = (diff / denom).fillna(0)
                        flagged = diff_pct > self.DISCREPANCY_THRESHOLD
                        if flagged.any():
                            for _, row in merged[flagged][[col, api_col]].iterrows():
                                discrepancies.append(
                                    {
                                        "table": table_name,
                                        "field": base,
                                        "csv": row[col],
                                        "api": row[api_col],
                                        "diff_pct": float(
                                            abs(row[col] - row[api_col]) / row[api_col]
                                        )
                                        if row[api_col] not in (0, None, pd.NA)
                                        else 0.0,
                                    }
                                )
        else:
            merged = pd.concat([working_api_df, working_csv_df], ignore_index=True, sort=False)

        merged["_source"] = merged.get("_source_api", merged.get("_source_csv", "merged"))
        merged.drop(
            columns=[c for c in merged.columns if c.startswith("_source_")],
            inplace=True,
            errors="ignore",
        )
        return merged, discrepancies

    def reconcile_conflicts(self, csv_value: Any, api_value: Any, field: str) -> Tuple[Any, str]:
        """Resolve conflicts according to priority rules."""
        if api_value is None and csv_value is None:
            return None, "missing"
        if api_value is None:
            return csv_value, "csv"
        if csv_value is None:
            return api_value, "api"

        try:
            csv_float = float(csv_value)
            api_float = float(api_value)
            if api_float == 0:
                return (csv_value, "csv") if csv_value else (api_value, "api")
            diff_pct = abs(csv_float - api_float) / abs(api_float)
            if diff_pct <= self.DISCREPANCY_THRESHOLD:
                return api_value, "api"
            return (api_value, "api") if "current" in field.lower() else (csv_value, "csv")
        except (TypeError, ValueError):
            return api_value, "api"


data_source_manager = DataSourceManager()
