"""Query clarification and user feedback nodes."""

import logging

from pocketflow import Node

logger = logging.getLogger(__name__)


class ClarifyQuery(Node):
    """Detect whether a user query references unknown tables or columns."""

    def prep(self, shared):
        """
        Extract the current question, schema string, and list of dataframe/table names from the shared state.

        Parameters:
            shared (dict): Shared runtime state expected to contain the keys:
                - "question": the user's question (str).
                - "schema_str": rendered schema description (str).
                - "dfs": mapping of dataframe/table names to their objects.

        Returns:
            tuple: (question, schema_str, table_names) where `question` is the question string,
            `schema_str` is the schema description string, and `table_names` is a list of
            keys from `shared["dfs"]`.
        """
        return shared["question"], shared["schema_str"], list(shared["dfs"].keys())

    def exec(self, prep_res):
        """
        Detects whether the user's question contains identifiers (words with underscores) that are not present in the provided schema.

        Parameters:
            prep_res (tuple): A tuple (question, schema, _table_names) where `question` is the user query string, `schema` is a string representation of available table/column names, and `_table_names` is a list of table names (unused).

        Returns:
            tuple: A pair (status, missing) where `status` is `"ambiguous"` if one or more identifier-like words from the question are missing from the schema and `"clear"` otherwise; `missing` is a list of the missing identifier strings when `status` is `"ambiguous"`, or `None` when `status` is `"clear"`.
        """
        question, schema, _table_names = prep_res
        question_lower = question.lower()
        suspicious_patterns = []

        words = question_lower.split()
        for word in words:
            if "_" in word and len(word) > 3 and word not in schema.lower():
                suspicious_patterns.append(word)

        if suspicious_patterns:
            schema_lower = schema.lower()
            truly_missing = [
                pattern
                for pattern in suspicious_patterns
                if pattern not in schema_lower
            ]
            if truly_missing:
                return "ambiguous", truly_missing

        return "clear", None

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Update shared state when the clarification step completes and indicate next flow status.

        Parameters:
            shared (dict): Mutable workflow state; may be updated with a user-facing final_text when references are missing.
            prep_res: Preparation result (not used by this method).
            exec_res (tuple): Execution result as (status, missing) where `status` is either "ambiguous" or "clear" and `missing` is a list or representation of missing schema identifiers.

        Returns:
            str: `"ambiguous"` if missing references were detected and `shared["final_text"]` was set, `"clear"` otherwise.
        """
        status, missing = exec_res
        if status == "ambiguous":
            shared["final_text"] = (
                "Your query references unknown columns or tables: "
                f"{missing}. Please clarify or check available schema."
            )
            return "ambiguous"
        return "clear"


class AskUser(Node):
    """Terminal node for ambiguous queries. In CLI mode, prompts user for clarification."""

    def prep(self, shared):
        """
        Assemble the values AskUser needs from the shared execution state for its exec step.

        Parameters:
            shared (dict): Shared runtime state; may contain keys "final_text", "question", "schema_str", and "is_cli".

        Returns:
            dict: A mapping with keys:
                - "final_text" (str): message to show the user (default "").
                - "question" (str): current question text (default "").
                - "schema_str" (str): schema description string (default "").
                - "is_cli" (bool): whether the environment is CLI (default False).
        """
        return {
            "final_text": shared.get("final_text", ""),
            "question": shared.get("question", ""),
            "schema_str": shared.get("schema_str", ""),
            "is_cli": shared.get("is_cli", False),
        }

    def exec(self, prep_res):
        """
        Prompt the user for a clarified question when running in CLI mode and return the chosen action.

        Parameters:
            prep_res (dict): Preparation result containing:
                - final_text (str): Message to show the user (defaults to "").
                - is_cli (bool): If True, prompt the user on the CLI; otherwise skip prompting.

        Returns:
            dict: An action dictionary with keys:
                - "action" (str): One of:
                    - "clarified": user provided a non-empty clarified question.
                    - "quit": user chose to quit or an interrupt/EOF occurred.
                    - "exit": not in CLI mode (no prompt performed).
                - "clarified_question" (str or None): The user's clarified question when action is "clarified", otherwise None.
        """
        prep_res.get("final_text", "")
        is_cli = prep_res.get("is_cli", False)

        if is_cli:
            try:
                user_input = input("> ").strip()
                if user_input.lower() in ["quit", "exit", "q"]:
                    return {"action": "quit", "clarified_question": None}
                if user_input:
                    return {"action": "clarified", "clarified_question": user_input}
            except (EOFError, KeyboardInterrupt):
                return {"action": "quit", "clarified_question": None}

        return {"action": "exit", "clarified_question": None}

    def post(self, shared, prep_res, exec_res) -> str:
        """
        Apply the user's response to the shared runtime state and determine the next node outcome.

        Parameters:
            shared (dict): Mutable shared state for the flow; may be updated when the user provides a clarified question or ends the session.
            prep_res: Unused in this post-processing step.
            exec_res (dict or None): Execution result from exec containing an 'action' key (e.g., "clarified", "quit", or other) and an optional 'clarified_question' string. If None, it is treated as {"action": "exit", "clarified_question": None}.

        Returns:
            str: "clarified" if the clarified question was applied and analysis should restart, "quit" if the session was terminated by the user, or "default" for all other outcomes.
        """
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
