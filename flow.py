from pocketflow import Flow
from nodes import (
    LoadData, SchemaInference, 
    ClarifyQuery, AskUser, EntityResolver, Planner,
    CodeGenerator, SafetyCheck, Executor, ErrorFixer,
    DeepAnalyzer, Visualizer, ResponseSynthesizer
)

def create_analyst_flow():
    """
    Creates the Enhanced Relational Data Analyst Flow.
    Includes EntityResolver, DeepAnalyzer, and ResponseSynthesizer for comprehensive analysis.
    """

    load = LoadData()
    schema = SchemaInference()
    clarify = ClarifyQuery()
    ask_user = AskUser()
    entity_resolver = EntityResolver()
    plan = Planner()

    code_gen = CodeGenerator()
    safety = SafetyCheck()
    executor = Executor()
    fixer = ErrorFixer()

    deep_analyzer = DeepAnalyzer()
    viz = Visualizer()
    synthesizer = ResponseSynthesizer()

    load >> schema >> clarify

    clarify - "ambiguous" >> ask_user
    clarify - "clear"     >> entity_resolver

    entity_resolver >> plan >> code_gen

    code_gen >> safety

    safety - "unsafe" >> code_gen
    safety - "safe"   >> executor

    executor - "error"   >> fixer
    fixer    - "fix"     >> code_gen
    fixer    - "give_up" >> synthesizer
    executor - "success" >> deep_analyzer

    deep_analyzer >> viz >> synthesizer

    return Flow(start=load)

if __name__ == "__main__":
    flow = create_analyst_flow()
    print("Flow created successfully.")
