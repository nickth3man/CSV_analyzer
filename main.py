from flow import create_analyst_flow

def main():
    # 1. Initialize the Shared Store
    # We start with the user's question.
    # 'dfs' and 'schema_str' will be populated by the LoadData and SchemaInference nodes.
    shared = {
        "question": "What is the average points scored by players on the Chicago team?",

        # Optional: Initialize state variables for clarity, though Nodes will create them if missing.
        "retry_count": 0,
        "exec_error": None,
        "history": [] 
    }

    # 2. Create the Flow
    # This wires together the 10 nodes including the Safety and Error loops.
    analyst_flow = create_analyst_flow()

    # 3. Run the Flow
    print("--- Starting Analyst Agent ---")
    print(f"User Question: {shared['question']}\n")

    # The flow executes starting from 'LoadData' and follows the transitions 
    # defined in flow.py until it reaches a terminal state.
    analyst_flow.run(shared)

    # 4. Final Output
    print("\n" + "="*30)
    print("         SESSION COMPLETE      ")
    print("="*30)

    if "final_text" in shared:
        print(f"\n🤖 Agent Response:\n{shared['final_text']}")
    else:
        print("\n⚠️ The flow finished but did not produce a final text response.")

    # Check if visualization was created
    if shared.get("chart_path"):
        print(f"\n📊 Chart saved to: {shared['chart_path']}")

if __name__ == "__main__":
    main()
