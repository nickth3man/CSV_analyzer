"""Query clarification and user feedback nodes.

This module handles query clarity checking and user interaction,
as specified in design.md Section 6.1.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import yaml  # type: ignore[import-untyped]
from pocketflow import Node

from src.backend.models import QueryIntent
from src.backend.utils.call_llm import call_llm
from src.backend.utils.logger import get_logger
from src.backend.utils.memory import get_memory


logger = logging.getLogger(__name__)


CLARIFY_QUERY_PROMPT = """You are analyzing if a user's NBA question is specific enough to query a database.

CLEAR queries have:
- Specific metrics (points, FG%, rebounds, assists)
- Time bounds (season, year, date range)
- Entity references (player name, team name)

AMBIGUOUS queries need clarification:
- "Who is the best?" → Best by what metric?
- "How did they do?" → Who is "they"? What timeframe?
- "Show me some stats" → Which stats? For whom?

Conversation context (may resolve ambiguity):
{conversation_history}

Current question: {question}

Output as YAML:
```yaml
intent: clear | ambiguous
reasoning: <one sentence explaining your decision>
clarification_questions:  # Only if ambiguous, 2-3 targeted questions
  - <question 1>
  - <question 2>
```
"""


class ClarifyQuery(Node):
    """Determine if the user's question is specific enough to answer.

    This node uses LLM to analyze if the query is specific enough for SQL
    generation. If ambiguous, it generates targeted clarification questions.
    """

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Read question and conversation history from shared store.

        Args:
            shared: The shared store.

        Returns:
            Dictionary with question and conversation_history.
        """
        question = shared.get("question", "")
        conversation_history = shared.get("conversation_history")
        if conversation_history is None:
            conversation_history = get_memory().get_context(n_turns=3).turns
            shared["conversation_history"] = conversation_history

        get_logger().log_node_start(
            "ClarifyQuery",
            {
                "question": question[:100],
                "history_turns": len(conversation_history),
            },
        )

        return {
            "question": question,
            "conversation_history": conversation_history,
        }

    def exec(self, prep_res: dict[str, Any]) -> dict[str, Any]:
        """Analyze query clarity using LLM.

        Args:
            prep_res: Dictionary with question and history.

        Returns:
            Dictionary with intent, reasoning, and clarification_questions.
        """
        question = prep_res["question"]
        conversation_history = prep_res["conversation_history"]

        history_text = self._format_conversation_history(conversation_history)

        prompt = CLARIFY_QUERY_PROMPT.format(
            question=question,
            conversation_history=history_text or "No previous conversation.",
        )

        response = call_llm(prompt)
        return self._parse_response(response)

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, Any],
    ) -> str:
        """Store intent and clarification questions in shared store.

        Args:
            shared: The shared store.
            prep_res: Result from prep().
            exec_res: Result from exec().

        Returns:
            Action string: "clear" or "ambiguous".
        """
        intent = exec_res.get("intent", QueryIntent.CLEAR)
        shared["intent"] = intent

        get_logger().log_node_end(
            "ClarifyQuery",
            {
                "intent": intent.value if hasattr(intent, "value") else str(intent),
                "reasoning": exec_res.get("reasoning", "")[:100],
            },
            "success",
        )

        if intent == QueryIntent.AMBIGUOUS:
            clarification_questions = exec_res.get("clarification_questions", [])
            shared["clarification_questions"] = clarification_questions

            if clarification_questions:
                shared["final_text"] = (
                    "I need a bit more information to answer your question:\n"
                    + "\n".join(f"- {q}" for q in clarification_questions)
                )
            else:
                shared["final_text"] = (
                    "Your question is too vague. Please be more specific about "
                    "what statistics, players, teams, or time periods you're interested in."
                )

            logger.info("Query is ambiguous: %s", exec_res.get("reasoning", ""))
            return "ambiguous"

        logger.info("Query is clear: %s", exec_res.get("reasoning", ""))
        return "clear"

    def _format_conversation_history(self, history: list[Any]) -> str:
        """Format conversation history for the prompt.

        Args:
            history: List of ConversationTurn objects.

        Returns:
            Formatted string representation.
        """
        if not history:
            return ""

        lines = []
        for i, turn in enumerate(history[-3:], 1):  # Last 3 turns
            q = turn.question if hasattr(turn, "question") else str(turn)
            a = turn.answer if hasattr(turn, "answer") else ""
            lines.append(f"Turn {i}:")
            lines.append(f"  Q: {q}")
            if a:
                truncated = a[:150] + "..." if len(a) > 150 else a
                lines.append(f"  A: {truncated}")

        return "\n".join(lines)

    def _parse_response(self, response: str) -> dict[str, Any]:
        """Parse YAML response from LLM.

        Args:
            response: Raw LLM response.

        Returns:
            Dictionary with intent, reasoning, and clarification_questions.
        """
        try:
            yaml_match = re.search(r"```yaml\s*(.*?)\s*```", response, re.DOTALL)
            yaml_str = yaml_match.group(1) if yaml_match else response.strip()

            result = yaml.safe_load(yaml_str)

            if not isinstance(result, dict):
                return {
                    "intent": QueryIntent.CLEAR,
                    "reasoning": "Parse failed, assuming clear",
                }

            intent_str = result.get("intent", "clear").lower()
            intent = (
                QueryIntent.AMBIGUOUS
                if intent_str == "ambiguous"
                else QueryIntent.CLEAR
            )

            reasoning = result.get("reasoning", "")
            clarification_questions = result.get("clarification_questions", [])

            if not isinstance(clarification_questions, list):
                clarification_questions = []

            return {
                "intent": intent,
                "reasoning": reasoning,
                "clarification_questions": clarification_questions,
            }

        except yaml.YAMLError as e:
            logger.warning("Failed to parse YAML response: %s", e)
            return {
                "intent": QueryIntent.CLEAR,
                "reasoning": "Parse error, assuming clear",
            }


class AskUser(Node):
    """Terminal node for ambiguous queries. Prompts user for clarification."""

    def prep(self, shared: dict[str, Any]) -> dict[str, Any]:
        """Prepare data for user interaction.

        Args:
            shared: The shared store.

        Returns:
            Dictionary with final_text, question, and is_cli flag.
        """
        return {
            "final_text": shared.get("final_text", ""),
            "question": shared.get("question", ""),
            "clarification_questions": shared.get("clarification_questions", []),
            "is_cli": shared.get("is_cli", False),
        }

    def exec(self, prep_res: dict[str, Any]) -> dict[str, Any]:
        """Prompt user for clarification in CLI mode.

        Args:
            prep_res: Dictionary with final_text and is_cli.

        Returns:
            Dictionary with action and clarified_question.
        """
        is_cli = prep_res.get("is_cli", False)

        if is_cli:
            final_text = prep_res.get("final_text", "")
            prompt = f"{final_text}\n> " if final_text else "> "

            try:
                user_input = input(prompt).strip()
                if user_input.lower() in ["quit", "exit", "q"]:
                    return {"action": "quit", "clarified_question": None}
                if user_input:
                    return {"action": "clarified", "clarified_question": user_input}
            except (EOFError, KeyboardInterrupt):
                return {"action": "quit", "clarified_question": None}

        return {"action": "exit", "clarified_question": None}

    def post(
        self,
        shared: dict[str, Any],
        prep_res: dict[str, Any],
        exec_res: dict[str, Any],
    ) -> str:
        """Process user response and determine next action.

        Args:
            shared: The shared store.
            prep_res: Result from prep().
            exec_res: Result from exec().

        Returns:
            Action string: "clarified", "quit", or "default".
        """
        if exec_res is None:
            exec_res = {"action": "exit", "clarified_question": None}

        action = exec_res.get("action", "exit")
        clarified_question = exec_res.get("clarified_question")

        if action == "clarified" and clarified_question:
            shared["question"] = clarified_question
            shared["execution_error"] = None
            shared["total_retries"] = 0
            shared["grader_retries"] = 0
            shared.pop("grader_feedback", None)
            shared.pop("sql_query", None)
            shared.pop("query_result", None)
            shared.pop("generation_attempts", None)
            shared.pop("query_plan", None)
            shared.pop("sub_query_results", None)
            shared.pop("sub_query_sqls", None)
            shared.pop("sub_query_tables", None)
            shared.pop("sub_query_errors", None)

            logger.info("Re-analyzing with clarified question: %s", clarified_question)
            return "clarified"

        if action == "quit":
            shared["final_text"] = "Session ended by user."
            logger.info("Session ended by user")
            return "quit"

        logger.info("Exiting AskUser: %s", shared.get("final_text", ""))
        return "default"
