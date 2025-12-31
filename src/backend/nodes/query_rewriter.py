"""QueryRewriter node for the NBA Data Analyst Agent.

This module normalizes queries and resolves conversational references,
as specified in design.md Section 6.2.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import yaml
from pocketflow import Node

from src.backend.models import ResolvedReferences
from src.backend.utils.call_llm import call_llm
from src.backend.utils.logger import get_logger
from src.backend.utils.memory import get_memory

logger = logging.getLogger(__name__)


QUERY_REWRITER_PROMPT = """You are rewriting a user's NBA question to be self-contained and suitable for SQL generation.

Your task:
1. Resolve pronouns and references ("they", "that team", "same stats", "how about X")
2. Expand abbreviations (e.g., "FG%" → "field goal percentage")
3. Normalize terminology to match database conventions
4. Produce a standalone query that can be understood without conversation context

Conversation History (most recent last):
{conversation_history}

Current Question: {question}

Reference Resolution Hints:
{resolution_hints}

Output as YAML:
```yaml
rewritten_query: <self-contained query>
resolved_entities:
  <original_reference>: <resolved_value>
reasoning: <brief explanation of changes made>
```

If no changes are needed, return the original question as rewritten_query.
"""


class QueryRewriter(Node):
    """Normalize queries and resolve conversational references.

    This node transforms user questions into self-contained queries
    suitable for SQL generation by resolving pronouns, expanding
    abbreviations, and normalizing terminology.
    """

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Read question and conversation history from shared store.

        Args:
            shared: The shared store containing question and history.

        Returns:
            Dictionary with question and conversation_history.
        """
        question = shared.get("question", "")
        conversation_history = shared.get("conversation_history")
        if conversation_history is None:
            conversation_history = get_memory().get_context(n_turns=5).turns
            shared["conversation_history"] = conversation_history

        get_logger().log_node_start(
            "QueryRewriter",
            {"question": question, "history_turns": len(conversation_history)},
        )

        return {
            "question": question,
            "conversation_history": conversation_history,
        }

    def exec(self, prep_res: dict[str, Any]) -> dict[str, Any]:
        """Rewrite the query using LLM and memory utilities.

        Args:
            prep_res: Dictionary with question and conversation_history.

        Returns:
            Dictionary with rewritten_query and resolved_references.
        """
        question = prep_res["question"]
        conversation_history = prep_res["conversation_history"]

        memory = get_memory()
        rule_based_resolution = memory.extract_references(question)

        history_text = self._format_conversation_history(conversation_history)
        resolution_hints = self._format_resolution_hints(rule_based_resolution)

        prompt = QUERY_REWRITER_PROMPT.format(
            question=question,
            conversation_history=history_text or "No previous conversation.",
            resolution_hints=resolution_hints or "No hints available.",
        )

        response = call_llm(prompt)
        result = self._parse_response(response, question)

        final_query = result.get("rewritten_query", question)
        resolved_entities = result.get("resolved_entities", {})

        if rule_based_resolution.resolved_entities:
            for ref, resolved in rule_based_resolution.resolved_entities.items():
                if ref not in resolved_entities:
                    resolved_entities[ref] = resolved
        resolved_entities = {
            str(ref): str(value)
            for ref, value in resolved_entities.items()
            if value is not None
        }

        return {
            "rewritten_query": final_query,
            "resolved_references": ResolvedReferences(
                original_query=question,
                expanded_query=final_query,
                resolved_entities=resolved_entities,
            ),
            "reasoning": result.get("reasoning", ""),
        }

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, Any],
    ) -> str:
        """Store rewritten query in shared store.

        Args:
            shared: The shared store.
            prep_res: Result from prep().
            exec_res: Result from exec().

        Returns:
            Action string "default" to continue to next node.
        """
        shared["rewritten_query"] = exec_res["rewritten_query"]
        shared["resolved_question"] = exec_res["rewritten_query"]
        shared["resolved_references"] = exec_res["resolved_references"]

        get_logger().log_node_end(
            "QueryRewriter",
            {
                "rewritten_query": exec_res["rewritten_query"],
                "entities_resolved": len(
                    exec_res["resolved_references"].resolved_entities
                ),
            },
            "success",
        )

        logger.info(
            "Query rewritten: '%s' -> '%s'",
            prep_res["question"],
            exec_res["rewritten_query"],
        )

        return "default"

    def _format_conversation_history(self, history: list[Any]) -> str:
        """Format conversation history for the prompt.

        Args:
            history: List of ConversationTurn objects.

        Returns:
            Formatted string representation of history.
        """
        if not history:
            return ""

        lines = []
        for i, turn in enumerate(history[-5:], 1):  # Last 5 turns
            q = turn.question if hasattr(turn, "question") else str(turn)
            a = turn.answer if hasattr(turn, "answer") else ""
            lines.append(f"Turn {i}:")
            lines.append(f"  Q: {q}")
            if a:
                lines.append(f"  A: {a[:200]}..." if len(a) > 200 else f"  A: {a}")

        return "\n".join(lines)

    def _format_resolution_hints(self, resolved: ResolvedReferences) -> str:
        """Format rule-based resolution hints for the prompt.

        Args:
            resolved: ResolvedReferences from memory utility.

        Returns:
            Formatted hints string.
        """
        if not resolved.resolved_entities:
            return ""

        lines = ["Detected references that may need resolution:"]
        for ref, value in resolved.resolved_entities.items():
            lines.append(f"  - '{ref}' might refer to '{value}'")

        return "\n".join(lines)

    def _parse_response(self, response: str, fallback_query: str) -> dict[str, Any]:
        """Parse YAML response from LLM.

        Args:
            response: Raw LLM response.
            fallback_query: Original query to use if parsing fails.

        Returns:
            Parsed dictionary with rewritten_query and resolved_entities.
        """
        try:
            yaml_match = re.search(r"```yaml\s*(.*?)\s*```", response, re.DOTALL)
            if yaml_match:
                yaml_str = yaml_match.group(1)
            else:
                yaml_str = response.strip()

            result = yaml.safe_load(yaml_str)

            if not isinstance(result, dict):
                return {"rewritten_query": fallback_query}

            if "rewritten_query" not in result:
                result["rewritten_query"] = fallback_query

            if "resolved_entities" not in result:
                result["resolved_entities"] = {}

            return result

        except yaml.YAMLError as e:
            logger.warning("Failed to parse YAML response: %s", e)
            return {"rewritten_query": fallback_query, "resolved_entities": {}}
