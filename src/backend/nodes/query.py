"""
Query clarification and user feedback nodes.
"""

import logging
from pocketflow import Node

logger = logging.getLogger(__name__)


class ClarifyQuery(Node):
    """Detect whether a user query references unknown tables or columns."""

    def prep(self, shared):
        return shared["question"], shared["schema_str"], list(shared["dfs"].keys())

    def exec(self, prep_res):
        question, schema, _table_names = prep_res
        question_lower = question.lower()
        suspicious_patterns = []

        words = question_lower.split()
        for word in words:
            if "_" in word and len(word) > 3 and word not in schema.lower():
                suspicious_patterns.append(word)

        if suspicious_patterns:
            schema_lower = schema.lower()
            truly_missing = [pattern for pattern in suspicious_patterns if pattern not in schema_lower]
            if truly_missing:
                return "ambiguous", truly_missing

        return "clear", None

    def post(self, shared, prep_res, exec_res):
        status, missing = exec_res
        if status == "ambiguous":
            shared["final_text"] = (
                "Your query references unknown columns or tables: "
                f"{missing}. Please clarify or check available schema."
            )
            return "ambiguous"
        return "clear"


class AskUser(Node):
    """
    Terminal node for ambiguous queries. In CLI mode, prompts user for clarification.
    """

    def prep(self, shared):
        return {
            "final_text": shared.get("final_text", ""),
            "question": shared.get("question", ""),
            "schema_str": shared.get("schema_str", ""),
            "is_cli": shared.get("is_cli", False),
        }

    def exec(self, prep_res):
        final_text = prep_res.get("final_text", "")
        is_cli = prep_res.get("is_cli", False)

        if is_cli:
            print(f"\n⚠️  {final_text}")
            print("\nPlease provide a clarified question (or type 'quit' to exit):")
            try:
                user_input = input("> ").strip()
                if user_input.lower() in ["quit", "exit", "q"]:
                    return {"action": "quit", "clarified_question": None}
                if user_input:
                    return {"action": "clarified", "clarified_question": user_input}
            except (EOFError, KeyboardInterrupt):
                return {"action": "quit", "clarified_question": None}

        return {"action": "exit", "clarified_question": None}

    def post(self, shared, prep_res, exec_res):
        if exec_res is None:
            exec_res = {"action": "exit", "clarified_question": None}

        action = exec_res.get("action", "exit")
        clarified_question = exec_res.get("clarified_question")

        if action == "clarified" and clarified_question:
            shared["question"] = clarified_question
            shared["exec_error"] = None
            shared["retry_count"] = 0
            shared.pop("entities", None)
            shared.pop("entity_map", None)
            shared.pop("cross_references", None)
            logger.info(f"Re-analyzing with clarified question: {clarified_question}")
            return "clarified"
        if action == "quit":
            shared["final_text"] = "Session ended by user."
            logger.info("Goodbye!")
            return "quit"

        logger.info(f"System: {shared.get('final_text', 'Ends')}")
        return "default"
