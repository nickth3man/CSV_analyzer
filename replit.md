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
- **Gradio Web Interface** (`app.py`): Mobile-first responsive design running on port 5000
  - Tabbed interface: Chat, Data, History, Help
  - Example questions for easy onboarding
  - Real-time status updates during analysis
  - Chart display for visualizations
  - Collapsible Settings and Data Profile sections
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
        - `app.py`: Gradio web interface (main entry point).
        - `main.py`: CLI entry point for running analysis without GUI.
        - `nodes.py`: Node definitions (18 nodes including EntityResolver, DeepAnalyzer, ResponseSynthesizer).
        - `flow.py`: Flow creation and connections.
        - `utils/`: Utility functions
          - `call_llm.py`: Standard LLM calls
          - `call_llm_streaming.py`: Streaming LLM calls for Gradio
          - `knowledge_store.py`: Persistent learning storage
        - `CSV/`: Data directory for CSV files
        - `requirements.txt`: Project dependencies (pocketflow, openai, gradio, matplotlib, pandas).
        - `docs/design.md`: High-level, no-code design documentation.

## Recent Changes (December 2024)
- Added **Gradio Frontend** (`app.py`) - comprehensive mobile-responsive web interface with:
  - **Chat Tab**: Streaming chat interface with status updates and chart display
  - **Data Tab**: CSV file upload, preview, and management
  - **History Tab**: Knowledge store viewer with learned patterns
  - **Help Tab**: Usage documentation and examples
  - **Settings**: 
    - API key input field for users to enter their OpenRouter API key
    - Dynamic model dropdown that fetches available models from OpenRouter API
    - "Refresh Models" button to update model list
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