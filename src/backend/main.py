"""Main entry point for running the NBA analyst flow from command line."""

from __future__ import annotations

import logging
import sys

from src.backend.config import get_config
from src.backend.flow import create_analyst_flow
from src.backend.utils.logger import get_logger


logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


DEFAULT_QUESTION = "Who led the league in points in 2023?"


def main() -> None:
    """Entry point for running the analyst flow from the command line."""
    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else DEFAULT_QUESTION
    config = get_config()

    shared: dict[str, object] = {
        "question": question,
        "conversation_history": [],
        "user_id": "cli",
        "total_retries": 0,
        "grader_retries": 0,
        "max_retries": config.resilience.max_retries,
        "is_cli": True,
    }

    trace_logger = get_logger()
    trace_id = trace_logger.start_trace(question=question, user_id="cli")

    analyst_flow = create_analyst_flow()

    logger.info("--- Starting NBA Analyst Agent ---")
    logger.info("User Question: %s\n", question)

    analyst_flow.run(shared)

    shared["execution_trace"] = trace_logger.end_trace(trace_id)

    logger.info("\n" + "=" * 30)
    logger.info("         SESSION COMPLETE      ")
    logger.info("=" * 30)

    final_answer = shared.get("final_answer") or shared.get("final_text")
    transparency_note = shared.get("transparency_note")

    if final_answer:
        logger.info("\nAnswer:\n%s", final_answer)
    if transparency_note:
        logger.info("\nHow I found this:\n%s", transparency_note)


if __name__ == "__main__":
    main()
