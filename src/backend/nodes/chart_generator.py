"""Chart generation node for visualizing query results."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from uuid import uuid4

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from pocketflow import Node

from src.backend.config import CHART_HISTORY_LIMIT, CHART_ROW_LIMIT
from src.backend.utils.logger import get_logger


logger = logging.getLogger(__name__)
matplotlib.use("Agg")

CHART_DIR = Path(__file__).parent.parent / "data" / "charts"


class ChartGenerator(Node):
    """Generate a simple chart from query results when possible."""

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        get_logger().log_node_start(
            "ChartGenerator",
            {"has_result": shared.get("query_result") is not None},
        )
        return {
            "query_result": shared.get("query_result"),
            "question": shared.get("question", ""),
            "chart_history": shared.get("chart_history", []),
        }

    def exec(self, prep_res: dict[str, Any]) -> dict[str, Any]:
        result = prep_res["query_result"]
        if not isinstance(result, pd.DataFrame) or result.empty:
            return {"chart_path": None, "chart_caption": None}

        numeric_cols = list(result.select_dtypes(include=["number"]).columns)
        if not numeric_cols:
            return {"chart_path": None, "chart_caption": None}

        label_cols = [col for col in result.columns if col not in numeric_cols]
        label_col = label_cols[0] if label_cols else None
        value_col = numeric_cols[0]

        data = result.head(CHART_ROW_LIMIT)
        labels = (
            data[label_col].astype(str).tolist()
            if label_col
            else [str(idx) for idx in data.index]
        )
        values = data[value_col].tolist()

        CHART_DIR.mkdir(parents=True, exist_ok=True)
        chart_id = uuid4().hex[:8]
        chart_path = CHART_DIR / f"chart_{chart_id}.png"

        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar(labels, values)
        ax.set_title(f"{value_col} by {label_col or 'index'}")
        ax.set_ylabel(value_col)
        if label_col:
            ax.set_xlabel(label_col)
        ax.tick_params(axis="x", rotation=45)
        fig.tight_layout()
        fig.savefig(chart_path, dpi=150)
        plt.close(fig)

        caption = f"{value_col} by {label_col or 'index'} (top {len(data)} rows)"
        return {"chart_path": str(chart_path), "chart_caption": caption}

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, Any],
    ) -> str:
        chart_path = exec_res.get("chart_path")
        chart_caption = exec_res.get("chart_caption")

        if chart_path:
            history = list(prep_res.get("chart_history") or [])
            history.append(chart_path)
            while len(history) > CHART_HISTORY_LIMIT:
                old_path = history.pop(0)
                try:
                    Path(old_path).unlink(missing_ok=True)
                except OSError:
                    logger.warning("Failed to remove old chart: %s", old_path)

            shared["chart_history"] = history
            shared["chart_path"] = chart_path
            shared["chart_caption"] = chart_caption
        else:
            shared["chart_path"] = None
            shared["chart_caption"] = None

        get_logger().log_node_end(
            "ChartGenerator",
            {"chart_path": chart_path or ""},
            "success",
        )
        return "default"
