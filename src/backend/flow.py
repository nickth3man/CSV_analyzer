import logging
from pocketflow import Flow

logger = logging.getLogger(__name__)

from backend.nodes import (
    AskUser,
    ClarifyQuery,
    CodeGenerator,
    ContextAggregator,
    CrossValidator,
    DataMerger,
    DataProfiler,
    DeepAnalyzer,
    EntityResolver,
    ErrorFixer,
    Executor,
    LoadData,
    NBAApiCodeGenerator,
    NBAApiDataLoader,
    Planner,
    ResponseSynthesizer,
    ResultValidator,
    SafetyCheck,
    SchemaInference,
    SearchExpander,
    Visualizer,
)

def create_analyst_flow():
    """
    Create the Enhanced Relational Data Analyst Flow.
    
    Constructs and wire together nodes for data ingestion, clarification, planning, code generation, safety checks, execution, validation, analysis, visualization, and response synthesis; the flow begins at the initial data loader.
    
    Returns:
        Flow: Flow object with start node set to the initial LoadData node.
    """

    load = LoadData()
    nba_loader = NBAApiDataLoader()
    merger = DataMerger()
    schema = SchemaInference()
    profiler = DataProfiler()
    clarify = ClarifyQuery()
    ask_user = AskUser()
    entity_resolver = EntityResolver()
    search_expander = SearchExpander()
    context_aggregator = ContextAggregator()
    plan = Planner()

    code_gen = CodeGenerator()
    api_code_gen = NBAApiCodeGenerator()
    safety = SafetyCheck()
    executor = Executor()
    fixer = ErrorFixer()

    result_validator = ResultValidator()
    cross_validator = CrossValidator()
    deep_analyzer = DeepAnalyzer()
    viz = Visualizer()
    synthesizer = ResponseSynthesizer()

    load - "default" >> nba_loader
    load - "no_data" >> ask_user
    nba_loader >> merger >> schema
    schema >> profiler >> clarify

    clarify - "ambiguous" >> ask_user
    clarify - "clear" >> entity_resolver

    # AskUser can re-enter the flow with a clarified question (CLI mode)
    ask_user - "clarified" >> entity_resolver
    # Other AskUser actions ("default", "quit") terminate the flow

    entity_resolver >> search_expander >> context_aggregator >> plan >> code_gen >> api_code_gen >> safety

    safety - "unsafe" >> code_gen
    safety - "safe" >> executor

    executor - "error" >> fixer
    fixer - "fix" >> code_gen
    fixer - "give_up" >> synthesizer
    executor - "success" >> result_validator

    result_validator >> cross_validator >> deep_analyzer >> viz >> synthesizer

    return Flow(start=load)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow = create_analyst_flow()
    logger.info("Flow created successfully.")
