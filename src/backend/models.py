"""Pydantic models for the NBA Data Analyst Agent.

This module defines all data models used throughout the system for type safety
and validation, as specified in design.md Section 5.1.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class QueryIntent(str, Enum):
    """Whether the user's query is clear or needs clarification."""

    CLEAR = "clear"
    AMBIGUOUS = "ambiguous"


class QueryComplexity(str, Enum):
    """Complexity classification for query planning."""

    SIMPLE = "simple"  # Single SQL query sufficient
    COMPLEX = "complex"  # Needs decomposition into sub-queries


class GradeStatus(str, Enum):
    """Quality assessment result from ResponseGrader."""

    PASS = "pass"  # noqa: S105  # nosec B105
    FAIL = "fail"


class TableMeta(BaseModel):
    """Metadata for a database table."""

    name: str
    description: str = ""
    row_count: int | None = None
    columns: list[str] = Field(default_factory=list)


class SubQuery(BaseModel):
    """A decomposed sub-query for complex questions."""

    id: str
    description: str
    depends_on: list[str] = Field(default_factory=list)
    sql: str | None = None
    result: dict[str, Any] | None = None


class QueryPlan(BaseModel):
    """Plan for executing a query (simple or decomposed)."""

    complexity: QueryComplexity
    sub_queries: list[SubQuery] = Field(default_factory=list)
    combination_strategy: str = "synthesize"


class ValidationResult(BaseModel):
    """Result of SQL syntax validation."""

    is_valid: bool
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class SQLGenerationAttempt(BaseModel):
    """Track each SQL generation attempt for debugging."""

    attempt_number: int
    sql: str
    validation: ValidationResult
    execution_error: str | None = None


class GraderFeedback(BaseModel):
    """Feedback from the ResponseGrader node."""

    status: GradeStatus
    issues: list[str] = Field(default_factory=list)
    suggestions: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)


class ConversationTurn(BaseModel):
    """A single turn in the conversation history."""

    question: str
    rewritten_query: str | None = None
    sql: str | None = None
    answer: str | None = None


class NodeExecution(BaseModel):
    """Execution details for a single node."""

    node: str
    status: str
    latency_ms: int
    attempts: int = 1


class ExecutionTrace(BaseModel):
    """Full trace of the execution for debugging."""

    trace_id: str
    question: str = ""
    nodes_executed: list[NodeExecution] = Field(default_factory=list)
    llm_calls: int = 0
    total_latency_ms: int = 0
    retries: int = 0
    cache_hits: int = 0


class ResolvedReferences(BaseModel):
    """Result of resolving pronouns and references in a query."""

    original_query: str
    expanded_query: str
    resolved_entities: dict[str, str] = Field(default_factory=dict)


class ConversationContext(BaseModel):
    """Context retrieved from conversation memory."""

    turns: list[ConversationTurn] = Field(default_factory=list)
    last_tables_used: list[str] = Field(default_factory=list)
    last_sql: str | None = None
