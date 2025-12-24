"""Main entry point for running the analyst flow from command line.

This module provides a CLI interface to run the NBA analyst flow
with a user-provided question or a default question.
"""

import logging
import sys

from src.backend.flow import create_analyst_flow


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


DEFAULT_QUESTION = "Compare the careers of LeBron James and Tracy McGrady"


def main() -> None:
    """Entry point for running the analyst flow from the command line."""
    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else DEFAULT_QUESTION

    shared: dict[str, object] = {
        "question": question,
        "retry_count": 0,
        "exec_error": None,
        "is_cli": True,  # Enable CLI mode for interactive ambiguity resolution
    }

    # 2. Create the Flow
    # This wires together the 10 nodes including the Safety and Error loops.
    analyst_flow = create_analyst_flow()

    logger.info("--- Starting Analyst Agent ---")
    logger.info("User Question: %s\n", shared["question"])

    analyst_flow.run(shared)

    logger.info("\n" + "=" * 30)
    logger.info("         SESSION COMPLETE      ")
    logger.info("=" * 30)

    if "final_text" in shared:
        logger.info("\nAgent Response:\n%s", shared["final_text"])
    else:
        logger.warning(
            "\nThe flow finished but did not produce a final text response."
        )

    # Check if visualization was created
    if shared.get("chart_path"):
        logger.info("\nChart saved to: %s", shared["chart_path"])


if __name__ == "__main__":
    main()
