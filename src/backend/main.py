import sys
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from src.backend.flow import create_analyst_flow

DEFAULT_QUESTION = "Compare the careers of LeBron James and Tracy McGrady"


def main():
    """Entry point for running the analyst flow from the command line."""
    question = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else DEFAULT_QUESTION

    shared = {
        "question": question,
        "retry_count": 0,
        "exec_error": None,
        "is_cli": True,  # Enable CLI mode for interactive ambiguity resolution
    }

    # 2. Create the Flow
    # This wires together the 10 nodes including the Safety and Error loops.
    analyst_flow = create_analyst_flow()

    logger.info("--- Starting Analyst Agent ---")
    logger.info(f"User Question: {shared['question']}\n")

    analyst_flow.run(shared)

    print("\n" + "=" * 30)
    print("         SESSION COMPLETE      ")
    print("=" * 30)

    if "final_text" in shared:
        print(f"\n🤖 Agent Response:\n{shared['final_text']}")
    else:
        logger.warning("\n⚠️ The flow finished but did not produce a final text response.")

    # Check if visualization was created
    if shared.get("chart_path"):
        logger.info(f"\n📊 Chart saved to: {shared['chart_path']}")

if __name__ == "__main__":
    main()
