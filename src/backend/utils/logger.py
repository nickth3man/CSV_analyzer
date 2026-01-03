"""Structured logging utility for the NBA Data Analyst Agent.

This module provides structured logging for debugging and analytics,
as specified in design.md Section 4.5.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

from src.backend.models import ExecutionTrace, NodeExecution


@dataclass
class LogContext:
    """Thread-local context for logging."""

    trace_id: str = ""
    node_name: str = ""
    start_time: float = 0.0
    user_id: str = ""


class StructuredLogger:
    """Structured logger for the NBA Data Analyst Agent.

    Emits structured JSON logs for debugging and analytics.
    Tracks execution traces per request.
    """

    def __init__(self, name: str = "nba_agent", level: int = logging.INFO) -> None:
        """Initialize the structured logger.

        Args:
            name: Logger name.
            level: Logging level.
        """
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)
        self._lock = threading.Lock()
        self._traces: dict[str, ExecutionTrace] = {}
        self._contexts: dict[int, LogContext] = {}  # Thread ID -> context

        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            )
            self._logger.addHandler(handler)

    def _get_context(self) -> LogContext:
        """Get the context for the current thread."""
        thread_id = threading.get_ident()
        if thread_id not in self._contexts:
            self._contexts[thread_id] = LogContext()
        return self._contexts[thread_id]

    def start_trace(self, question: str = "", user_id: str | None = None) -> str:
        """Start a new execution trace.

        Args:
            question: The user's question.

        Returns:
            The trace ID.
        """
        trace_id = str(uuid.uuid4())[:8]
        with self._lock:
            self._traces[trace_id] = ExecutionTrace(
                trace_id=trace_id,
                question=question,
            )
        context = self._get_context()
        context.trace_id = trace_id
        context.start_time = time.time()
        context.user_id = user_id or ""

        self._emit_log(
            event="trace_start",
            trace_id=trace_id,
            question=question,
        )
        return trace_id

    def end_trace(self, trace_id: str | None = None) -> ExecutionTrace | None:
        """End an execution trace.

        Args:
            trace_id: The trace ID. If None, uses current context.

        Returns:
            The completed execution trace.
        """
        context = self._get_context()
        trace_id = trace_id or context.trace_id

        with self._lock:
            trace = self._traces.get(trace_id)
            if trace:
                trace.total_latency_ms = int((time.time() - context.start_time) * 1000)
                self._emit_log(
                    event="trace_end",
                    trace_id=trace_id,
                    total_latency_ms=trace.total_latency_ms,
                    nodes_executed=len(trace.nodes_executed),
                    llm_calls=trace.llm_calls,
                    retries=trace.retries,
                )
                return trace
        return None

    def log_node_start(self, node: str, inputs: dict[str, Any] | None = None) -> None:
        """Log node entry.

        Args:
            node: Node name.
            inputs: Input data for the node.
        """
        context = self._get_context()
        context.node_name = node
        context.start_time = time.time()

        self._emit_log(
            event="node_start",
            trace_id=context.trace_id,
            node=node,
            inputs=self._truncate_inputs(inputs or {}),
        )

    def log_node_end(
        self,
        node: str,
        outputs: dict[str, Any] | None = None,
        status: str = "success",
        attempts: int = 1,
    ) -> None:
        """Log node exit.

        Args:
            node: Node name.
            outputs: Output data from the node.
            status: Execution status.
            attempts: Number of attempts made.
        """
        context = self._get_context()
        latency_ms = int((time.time() - context.start_time) * 1000)

        with self._lock:
            trace = self._traces.get(context.trace_id)
            if trace:
                trace.nodes_executed.append(
                    NodeExecution(
                        node=node,
                        status=status,
                        latency_ms=latency_ms,
                        attempts=attempts,
                    )
                )
                if attempts > 1:
                    trace.retries += attempts - 1

        self._emit_log(
            event="node_end",
            trace_id=context.trace_id,
            node=node,
            outputs=self._truncate_inputs(outputs or {}),
            status=status,
            latency_ms=latency_ms,
            attempts=attempts,
        )

    def log_llm_call(
        self,
        prompt: str,
        response: str,
        latency_ms: int,
        cached: bool = False,
        model: str = "",
    ) -> None:
        """Log LLM interaction.

        Args:
            prompt: The prompt sent to the LLM.
            response: The response received.
            latency_ms: Latency in milliseconds.
            cached: Whether the response was cached.
            model: The model used.
        """
        context = self._get_context()

        with self._lock:
            trace = self._traces.get(context.trace_id)
            if trace:
                trace.llm_calls += 1
                if cached:
                    trace.cache_hits += 1

        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()[:8]

        self._emit_log(
            event="llm_call",
            trace_id=context.trace_id,
            prompt_hash=prompt_hash,
            response_length=len(response),
            latency_ms=latency_ms,
            cached=cached,
            model=model,
        )

    def log_sql_execution(
        self,
        sql: str,
        row_count: int,
        latency_ms: int,
        error: str | None = None,
    ) -> None:
        """Log SQL execution.

        Args:
            sql: The SQL query.
            row_count: Number of rows returned.
            latency_ms: Latency in milliseconds.
            error: Error message if failed.
        """
        context = self._get_context()
        sql_hash = hashlib.sha256(sql.encode()).hexdigest()[:8]
        sql_preview = sql if len(sql) <= 500 else sql[:500] + "..."

        self._emit_log(
            event="sql_execution",
            trace_id=context.trace_id,
            sql_hash=sql_hash,
            sql_preview=sql_preview,
            row_count=row_count,
            latency_ms=latency_ms,
            error=error,
            status="error" if error else "success",
        )

    def log_retry(self, node: str, attempt: int, error: str) -> None:
        """Log retry attempt.

        Args:
            node: Node name.
            attempt: Attempt number.
            error: Error that caused the retry.
        """
        context = self._get_context()

        self._emit_log(
            event="retry",
            trace_id=context.trace_id,
            node=node,
            attempt=attempt,
            error=error,
        )

    def log_circuit_open(self, service: str, failure_count: int) -> None:
        """Log circuit breaker opening.

        Args:
            service: Service name.
            failure_count: Number of failures.
        """
        context = self._get_context()

        self._emit_log(
            event="circuit_open",
            trace_id=context.trace_id,
            service=service,
            failure_count=failure_count,
        )

    def get_trace(self, trace_id: str | None = None) -> ExecutionTrace | None:
        """Get the execution trace.

        Args:
            trace_id: The trace ID. If None, uses current context.

        Returns:
            The execution trace.
        """
        context = self._get_context()
        trace_id = trace_id or context.trace_id

        with self._lock:
            return self._traces.get(trace_id)

    def _emit_log(self, event: str, **kwargs: Any) -> None:
        """Emit a structured log entry.

        Args:
            event: Event type.
            **kwargs: Additional log fields.
        """
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "event": event,
            **kwargs,
        }
        context = self._get_context()
        if context.user_id and "user_id" not in log_entry:
            log_entry["user_id"] = context.user_id

        try:
            log_json = json.dumps(log_entry, default=str)
        except (TypeError, ValueError):
            log_json = json.dumps(
                {"event": event, "error": "Failed to serialize log entry"}
            )

        if event in ("node_end", "trace_end"):
            if kwargs.get("status") == "error":
                self._logger.error(log_json)
            else:
                self._logger.info(log_json)
        elif event in ("retry", "circuit_open"):
            self._logger.warning(log_json)
        else:
            self._logger.debug(log_json)

    def _truncate_inputs(
        self, data: dict[str, Any], max_length: int = 200
    ) -> dict[str, Any]:
        """Truncate long values in log data.

        Args:
            data: Data to truncate.
            max_length: Maximum string length.

        Returns:
            Truncated data.
        """
        result: dict[str, Any] = {}
        for key, value in data.items():
            if isinstance(value, str) and len(value) > max_length:
                result[key] = value[:max_length] + "..."
            elif isinstance(value, dict):
                result[key] = self._truncate_inputs(value, max_length)
            elif isinstance(value, list) and len(value) > 10:
                result[key] = f"[{len(value)} items]"
            else:
                result[key] = value
        return result


@lru_cache(maxsize=1)
def get_logger() -> StructuredLogger:
    """Get the global structured logger instance.

    Returns:
        The structured logger.
    """
    return StructuredLogger()
