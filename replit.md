# Agentic Coding: Humans Design, Agents Code!

## Overview
This project, "Pocket Flow," is a minimalist LLM framework designed for agentic coding, enabling humans to design and AI agents to implement. It aims to provide a lightweight yet expressive foundation for building complex LLM applications, including agents, task decomposition, RAG, and more, with a core abstraction centered around a Graph and Shared Store. The framework promotes an iterative development process where humans define high-level requirements and flows, and AI agents handle the detailed implementation, optimization, and reliability aspects. The business vision is to streamline the development of LLM-powered systems by fostering a collaborative workflow between human designers and AI implementers, reducing complexity and accelerating deployment.

## User Preferences
- **General Working Preferences**: Always start with a small and simple solution, design at a high level (`docs/design.md`) before implementation, and frequently ask humans for feedback and clarification.
- **Problem Solving Approach**: Focus on keeping solutions user-centric, explaining problems from the user's perspective, and balancing complexity with impact to deliver high-value features early.
- **Workflow Preferences**: Prefer an iterative development process, expecting to repeat design, utility implementation, data design, node design, and initial implementation steps multiple times.
- **Coding Style Preferences**: Avoid complex features and full-scale type checking in initial implementations. Prioritize fast failure to quickly identify weak points.
- **Interaction Preferences**: Thoroughly understand the problem and potential solution by manually solving example inputs to develop intuition before building LLM systems. If humans can't specify the flow, AI Agents can't automate it. Avoid exception handling within utility functions; let the Node's built-in retry mechanism handle failures. When designing data, try to minimize data redundancy.

## System Architecture
The core of Pocket Flow is modeled as a **Graph + Shared Store**.

### UI/UX Decisions
- **Chainlit Web Interface** (`app.py` with modular `frontend/` components): Modern chat-first UI running on port 5000
  - Conversational interface with action buttons
  - Settings panel (gear icon) for API key and model selection
  - File upload via paperclip icon or `/upload` command
  - Commands: `/tables`, `/preview <name>`, `/delete <name>`, `/schema`, `/knowledge`, `/help`
  - Real-time step indicators during analysis (Loading Data, Analyzing Schema, Running Analysis)
  - Inline chart display in chat messages
  - Example questions for easy onboarding
  - Modular architecture with separate modules for config, caching, data utilities, commands, actions, and handlers
- Mermaid diagrams are used for visualizing workflow in documentation.

### Technical Implementations
- **Core Abstractions**:
    - **Node**: The smallest building block, consisting of `prep()`, `exec()`, and `post()` steps for separation of concerns. Supports fault tolerance via `max_retries` and `wait` parameters, and graceful fallback with `exec_fallback()`.
    - **Flow**: Orchestrates nodes, allowing sequential, branching, and looping logic based on "Actions" returned by `post()`. Supports nested flows for powerful composition.
    - **Shared Store**: A global data structure (typically an in-memory dictionary) for communication between nodes, separating data schema from compute logic.
    - **Params**: Local, ephemeral dictionaries for per-Node or per-Flow configuration, primarily used for identifiers in Batch modes. Immutable during a Node's run cycle.
    - **BatchNode/BatchFlow**: Facilitates processing large inputs or running flows multiple times, ideal for chunk-based or iterative tasks. Supports nested batches.
    - **AsyncNode/AsyncFlow**: Enables asynchronous operations (`prep_async()`, `exec_async()`, `post_async()`) for I/O-friendly tasks like fetching data or async LLM calls.
    - **AsyncParallelBatchNode/AsyncParallelBatchFlow**: Runs multiple Async Nodes/Flows concurrently to improve performance by overlapping I/O.
- **Design Patterns**: The framework supports implementing common LLM design patterns:
    - **Agent**: Nodes take dynamic actions based on context, using branching and prompt engineering for decision-making. Emphasizes context management and a well-structured, unambiguous action space.
    - **Workflow**: Decomposes complex tasks into a chain of multiple nodes, balancing granularity for effective LLM calls.
    - **RAG (Retrieval Augmented Generation)**: A two-stage pipeline for question answering, involving offline indexing (chunking, embedding, storing in vector DB) and online query and answer generation.
    - **Map Reduce**: Breaks down large data tasks into smaller, independent parts using `BatchNode` for mapping and subsequent nodes for reduction/aggregation.
    - **Structured Output**: Guides LLMs to produce specific data structures (e.g., YAML) using prompt engineering and validation. YAML is preferred over JSON for better handling of escaping and newlines.
- **File Structure**:
    - `my_project/`
        - `app.py`: Chainlit web interface (main entry point).
        - `main.py`: CLI entry point for running analysis without GUI.
        - `nodes.py`: Node definitions (18 nodes including EntityResolver, DeepAnalyzer, ResponseSynthesizer).
        - `flow.py`: Flow creation and connections.
        - `frontend/`: Modular frontend components
          - `__init__.py`: Module initialization
          - `config.py`: Configuration and constants
          - `cache.py`: Dataframe caching
          - `data_utils.py`: Data loading and schema utilities
          - `knowledge_utils.py`: Knowledge store utilities
          - `commands.py`: Command handling
          - `actions.py`: Action callbacks
          - `steps.py`: Analysis pipeline steps
          - `handlers.py`: Main event handlers
        - `chainlit.md`: Chainlit welcome markdown displayed to users.
        - `.chainlit/config.toml`: Chainlit configuration (file upload, CoT display, etc).
        - `utils/`: Utility functions
          - `call_llm.py`: Standard LLM calls
          - `call_llm_streaming.py`: Streaming LLM calls
          - `knowledge_store.py`: Persistent learning storage
        - `CSV/`: Data directory for CSV files
        - `requirements.txt`: Project dependencies (pocketflow, openai, matplotlib, pandas, chainlit).
        - `docs/design.md`: High-level, no-code design documentation.

## Recent Changes (December 2024)
- **Refactored to modular frontend architecture** (`app.py` with `frontend/` components) - Organized Chainlit interface into reusable modules:
  - Config module for constants and model filtering
  - Cache module for efficient dataframe loading
  - Data utilities for schema and profiling
  - Separate modules for commands, actions, and handlers
  - Improved maintainability and testability
- **Migrated to Chainlit UI** - Modern chat-first interface replacing Gradio:
  - Action buttons for Upload CSV, Tables, Schema, Help
  - ChatSettings panel with model selection and API key input
  - Slash commands for data management (`/upload`, `/tables`, `/preview`, `/delete`, `/schema`, `/knowledge`, `/help`)
  - Step decorators for visual progress tracking during analysis
  - Inline chart display with cl.Image
  - File upload via AskFileMessage or spontaneous upload
  - Proper Chainlit config in `.chainlit/config.toml`
- Added **Streaming LLM utility** (`utils/call_llm_streaming.py`) for real-time token streaming
- Added **EntityResolver** node for discovering entities across tables using first_name/last_name matching
- Added **DeepAnalyzer** node for comprehensive statistical analysis with data validation
- Added **ResponseSynthesizer** node for generating narrative responses with honest data gap warnings
- Added **KnowledgeStore** utility for persistent entity mappings and query patterns (JSON-based)
- Added pre-built comparison template in CodeGenerator to prevent merge/dtype errors
- Improved data integrity: system honestly reports missing data instead of fabricating facts
- Successfully tested complex comparative queries ("Compare LeBron James and Tracy McGrady")

### Feature Specifications
- **Fault Tolerance**: Nodes can retry `exec()` method upon exceptions and provide fallback mechanisms.
- **Modularity**: Emphasis on small, reusable nodes and composable flows.
- **Flexibility**: No built-in utilities to avoid vendor lock-in; users are encouraged to implement their own.
- **Logging**: Recommended throughout the code for debugging.

## External Dependencies
- **LLM Providers**:
    - OpenAI
    - Anthropic (Claude)
    - Google (Generative AI Studio / PaLM API, Gemini)
    - Azure OpenAI
    - Ollama (for local LLMs)
- **Python Libraries**:
    - `PyYAML`
    - `pocketflow` (the framework itself)
    - `openai`
    - `anthropic`
    - `google-generativeai` (implied by Google example)
    - `ollama` (implied by Ollama example)
- **Vector Databases**: Used for RAG (e.g., FAISS or other vector DBs). The document does not specify a particular one but mentions the concept.
- **Web Search**: Implied by the "Search Agent" example and `search_web.py` utility.
- **General APIs**: External utility functions are envisioned to interact with various real-world systems like Slack, email services, etc.