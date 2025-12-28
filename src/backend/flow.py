"""Flow module for the NBA Expert data analyst workflow.

This module creates and configures the main analyst flow by wiring together
all the processing nodes for data ingestion, query clarification, code
generation, execution, and response synthesis.

# TODO (Architecture): Migrate to AsyncFlow for better performance
# Current flow is synchronous, blocking on each node execution.
# PocketFlow supports AsyncFlow which would enable:
#   1. Parallel data loading (CSV + NBA API simultaneously)
#   2. Non-blocking LLM calls
#   3. Streaming responses to the frontend
# Migration steps:
#   1. Convert nodes to async: implement prep_async, exec_async, post_async
#   2. Change Flow to AsyncFlow
#   3. Update main.py and handlers.py to use asyncio.run() or await
# Example:
#   class LoadData(AsyncNode):
#       async def exec_async(self, prep_res):
#           return await load_csvs_async(prep_res)
#   flow = AsyncFlow(start=load)
#   await flow.run_async(shared)

# TODO (Architecture): Add parallel data loading sub-flow
# LoadData and NBAApiDataLoader could run in parallel:
#   from pocketflow import AsyncParallelBatchFlow
#   class ParallelDataLoader(AsyncParallelBatchFlow):
#       async def prep_async(self, shared):
#           return [
#               {"type": "csv", "path": shared["data_dir"]},
#               {"type": "api", "question": shared["question"]}
#           ]
# This would reduce initial loading time significantly.

# TODO (Feature): Add batch question processing
# Enable processing multiple questions in a single flow run:
#   class BatchQuestionFlow(BatchFlow):
#       def prep(self, shared):
#           return [{"question": q} for q in shared["questions"]]
# Useful for automated analysis or API endpoints.

# TODO (Reliability): Add flow-level timeout
# Currently only individual executors have timeouts. Add flow-level:
#   FLOW_TIMEOUT = 300  # 5 minutes max for entire flow
#   async def run_with_timeout(flow, shared):
#       return await asyncio.wait_for(flow.run_async(shared), FLOW_TIMEOUT)

# TODO (Observability): Add tracing and metrics
# Instrument the flow for observability:
#   - Trace each node execution with OpenTelemetry
#   - Record timing, success/failure, and payload sizes
#   - Enable distributed tracing for debugging
# Example with OpenTelemetry:
#   from opentelemetry import trace
#   tracer = trace.get_tracer("nba_expert")
#   with tracer.start_as_current_span("flow.run"):
#       flow.run(shared)
"""

import logging

from pocketflow import Flow

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


logger = logging.getLogger(__name__)


def create_analyst_flow() -> Flow:
    """Create the Enhanced Relational Data Analyst Flow.

    Constructs and wire together nodes for data ingestion, clarification,
    planning, code generation, safety checks, execution, validation,
    analysis, visualization, and response synthesis; the flow begins at
    the initial data loader.

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

    entity_resolver >> search_expander >> context_aggregator >> plan >> code_gen
    code_gen >> api_code_gen >> safety

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
