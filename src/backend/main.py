import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "src"
for path in (SRC_PATH, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from backend.flow import create_analyst_flow

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

    print("--- Starting Analyst Agent ---")
    print(f"User Question: {shared['question']}\n")

    analyst_flow.run(shared)

    print("\n" + "=" * 30)
    print("         SESSION COMPLETE      ")
    print("=" * 30)

    if "final_text" in shared:
        print(f"\n🤖 Agent Response:\n{shared['final_text']}")
    else:
        print("\n⚠️ The flow finished but did not produce a final text response.")

    # Check if visualization was created
    if shared.get("chart_path"):
        print(f"\n📊 Chart saved to: {shared['chart_path']}")

if __name__ == "__main__":
    main()
