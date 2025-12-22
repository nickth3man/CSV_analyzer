from pocketflow import Flow
from nodes import (
    # Initialization Nodes
    LoadData, SchemaInference, 
    # Logic & Planning Nodes
    ClarifyQuery, AskUser, Planner,
    # Execution Loop Nodes
    CodeGenerator, SafetyCheck, Executor, ErrorFixer,
    # Output Nodes
    Visualizer, ResponseFormatter
)

def create_analyst_flow():
    """
    Creates the Relational Data Analyst Flow with 10 nodes.
    Includes cycles for Safety Checks and Error Correction.
    """

    # --- 1. Instantiate All Nodes ---
    load = LoadData()
    schema = SchemaInference()
    clarify = ClarifyQuery()
    ask_user = AskUser()        # Terminal node for ambiguous queries
    plan = Planner()

    code_gen = CodeGenerator()
    safety = SafetyCheck()
    executor = Executor()
    fixer = ErrorFixer()

    viz = Visualizer()
    formatter = ResponseFormatter()

    # --- 2. Wire the Linear Setup Phase ---
    # Load Data -> Infer Schema -> Check Ambiguity
    load >> schema >> clarify

    # --- 3. Wire the Decision Branch (Clarification) ---
    # If the question is unclear, stop and ask the user.
    # If clear, proceed to planning.
    clarify - "ambiguous" >> ask_user
    clarify - "clear"     >> plan

    # --- 4. Wire the Planning Phase ---
    # Plan -> Write Initial Code
    plan >> code_gen

    # --- 5. Wire the Safety Loop ---
    # Code -> Safety Check
    code_gen >> safety

    # If Unsafe: Go back to CodeGenerator to rewrite (removing malicious/bad parts)
    # If Safe: Proceed to Execution
    safety - "unsafe" >> code_gen
    safety - "safe"   >> executor

    # --- 6. Wire the Error Correction Loop ---
    # If Execution Fails: Go to Fixer -> Back to CodeGenerator
    # If Execution Succeeds: Proceed to Visualization
    executor - "error"   >> fixer
    fixer    - "fix"     >> code_gen
    executor - "success" >> viz

    # --- 7. Wire the Output Phase ---
    # Visualize -> Format Final Response
    viz >> formatter

    # --- 8. Create and Return Flow ---
    return Flow(start=load)

if __name__ == "__main__":
    # Simple test to visualize the graph structure if printed
    flow = create_analyst_flow()
    print("Flow created successfully.")
