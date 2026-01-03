from __future__ import annotations

import re
from typing import Any

import pandas as pd


class DataSourceManager:
    """Coordinates CSV + NBA API dataframes and resolves conflicts."""

    DISCREPANCY_THRESHOLD = 0.05

    def detect_query_entities(self, question: str) -> list[str]:
        """Lightweight entity detection to inform API endpoint choices."""
        if not question:
            return []
        candidates = re.findall(r"[A-Z][a-z]+(?:\s[A-Z][a-z]+)*", question)
        seen: list[str] = []
        for candidate in candidates:
            if candidate not in seen:
                seen.append(candidate)
        return seen

    def determine_api_endpoints(
        self,
        entities: list[str],
        question: str,
    ) -> list[dict[str, Any]]:
        """Map entities + intent to candidate endpoints."""
        endpoints: list[dict[str, Any]] = []
        q_lower = question.lower()

        self._add_scoreboard_endpoints(q_lower, endpoints)
        self._add_comparison_endpoints(q_lower, entities, endpoints)
        self._add_roster_endpoints(q_lower, entities, endpoints)
        self._add_game_log_endpoints(q_lower, entities, endpoints)

        if not endpoints:
            self._add_default_endpoints(entities, endpoints)

        return endpoints

    def _add_endpoint(
        self,
        endpoints: list[dict[str, Any]],
        name: str,
        params: dict[str, Any] | None = None,
    ) -> None:
        endpoints.append({"name": name, "params": params or {}})

    def _add_scoreboard_endpoints(self, q_lower: str, endpoints: list[dict[str, Any]]):
        if any(token in q_lower for token in ["score", "today", "live"]):
            self._add_endpoint(endpoints, "scoreboard", {})

    def _add_comparison_endpoints(
        self,
        q_lower: str,
        entities: list[str],
        endpoints: list[dict[str, Any]],
    ) -> None:
        if any(token in q_lower for token in ["compare", "versus", "vs", "better", "greater"]):
            for ent in entities:
                self._add_endpoint(endpoints, "player_career", {"entity": ent})
                self._add_endpoint(endpoints, "league_leaders", {"entity": ent})

    def _add_roster_endpoints(
        self,
        q_lower: str,
        entities: list[str],
        endpoints: list[dict[str, Any]],
    ) -> None:
        if "lineup" in q_lower or "roster" in q_lower:
            for ent in entities:
                self._add_endpoint(endpoints, "common_team_roster", {"entity": ent})

    def _add_game_log_endpoints(
        self,
        q_lower: str,
        entities: list[str],
        endpoints: list[dict[str, Any]],
    ) -> None:
        if "game log" in q_lower or "recent games" in q_lower:
            for ent in entities:
                self._add_endpoint(endpoints, "player_game_log", {"entity": ent})

    def _add_default_endpoints(
        self,
        entities: list[str],
        endpoints: list[dict[str, Any]],
    ) -> None:
        for ent in entities:
            self._add_endpoint(endpoints, "player_career", {"entity": ent})

    def merge_data_sources(
        self,
        csv_data: dict[str, pd.DataFrame],
        api_data: dict[str, pd.DataFrame],
    ) -> tuple[dict[str, pd.DataFrame], list[dict[str, Any]], dict[str, str]]:
        """Merge CSV + API dataframes with source tracking and discrepancies."""
        merged: dict[str, pd.DataFrame] = {}
        discrepancies: list[dict[str, Any]] = []
        sources: dict[str, str] = {}

        csv_data = csv_data or {}
        api_data = api_data or {}

        for name, df in csv_data.items():
            merged[name] = df.copy()
            merged[name]["_source"] = "csv"
            sources[name] = "csv"

        for name, df in api_data.items():
            if name in merged:
                merged_df, table_discrepancies = self._merge_table(
                    merged[name],
                    df,
                    name,
                )
                merged[name] = merged_df
                discrepancies.extend(table_discrepancies)
                sources[name] = "merged"
            else:
                merged[name] = df.copy()
                merged[name]["_source"] = "api"
                sources[name] = "api"

        return merged, discrepancies, sources

    def _merge_table(
        self,
        csv_df: pd.DataFrame,
        api_df: pd.DataFrame,
        table_name: str,
    ) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
        """Merge a single table and record discrepancies."""
        discrepancies: list[dict[str, Any]] = []
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
                working_api_df,
                on=key,
                how="outer",
                suffixes=("_csv", "_api"),
            )
            numeric_cols = [
                c for c in merged.columns if pd.api.types.is_numeric_dtype(merged[c])
            ]
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
                                        "diff_pct": (
                                            float(
                                                abs(row[col] - row[api_col])
                                                / row[api_col],
                                            )
                                            if row[api_col] not in (0, None, pd.NA)
                                            else 0.0
                                        ),
                                    },
                                )
        else:
            merged = pd.concat(
                [working_api_df, working_csv_df],
                ignore_index=True,
                sort=False,
            )

        merged["_source"] = merged.get(
            "_source_api",
            merged.get("_source_csv", "merged"),
        )
        merged = merged.drop(
            columns=[c for c in merged.columns if c.startswith("_source_")],
            errors="ignore",
        )
        return merged, discrepancies

    def reconcile_conflicts(
        self,
        csv_value: Any,
        api_value: Any,
        field: str,
    ) -> tuple[Any, str]:
        """Resolve conflicts according to priority rules."""
        source = "api"
        value = api_value

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
                value = api_value
                source = "api"
            else:
                value, source = (
                    (api_value, "api")
                    if "current" in field.lower()
                    else (csv_value, "csv")
                )
        except (TypeError, ValueError):
            value, source = api_value, "api"

        return value, source


data_source_manager = DataSourceManager()
