from pocketflow import Flow
from nodes import (
    LoadData, SchemaInference, DataProfiler,
    ClarifyQuery, AskUser, EntityResolver, SearchExpander,
    ContextAggregator, Planner,
    CodeGenerator, SafetyCheck, Executor, ErrorFixer,
    ResultValidator, DeepAnalyzer, Visualizer, ResponseSynthesizer
)

def create_analyst_flow():
    """
    Creates the Enhanced Relational Data Analyst Flow.
    Now includes DataProfiler, SearchExpander, ContextAggregator, and ResultValidator
    for deeper search and better inter-node communication.
    """

    load = LoadData()
    schema = SchemaInference()
    profiler = DataProfiler()
    clarify = ClarifyQuery()
    ask_user = AskUser()
    entity_resolver = EntityResolver()
    search_expander = SearchExpander()
    context_aggregator = ContextAggregator()
    plan = Planner()

    code_gen = CodeGenerator()
    safety = SafetyCheck()
    executor = Executor()
    fixer = ErrorFixer()

    result_validator = ResultValidator()
    deep_analyzer = DeepAnalyzer()
    viz = Visualizer()
    synthesizer = ResponseSynthesizer()

    load >> schema >> profiler >> clarify

    clarify - "ambiguous" >> ask_user
    clarify - "clear"     >> entity_resolver

    entity_resolver >> search_expander >> context_aggregator >> plan >> code_gen

    code_gen >> safety

    safety - "unsafe" >> code_gen
    safety - "safe"   >> executor

    executor - "error"   >> fixer
    fixer    - "fix"     >> code_gen
    fixer    - "give_up" >> synthesizer
    executor - "success" >> result_validator

    result_validator >> deep_analyzer >> viz >> synthesizer

    return Flow(start=load)

if __name__ == "__main__":
    flow = create_analyst_flow()
    print("Flow created successfully.")
