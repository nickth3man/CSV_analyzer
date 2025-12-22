# Design Doc: Relational Data Analyst Agent

> Please DON'T remove notes for AI

## Requirements

> Notes for AI: Keep it simple and clear.
> If the requirements are abstract, write concrete user stories

The Relational Data Analyst Agent is an LLM-powered system that allows users to ask natural language questions about CSV data. The agent:

1. **Loads CSV files** from a designated folder (`CSV/`)
2. **Infers schemas** from the loaded dataframes
3. **Plans and generates code** to answer user questions
4. **Executes code safely** in a sandboxed environment
5. **Handles errors gracefully** with automatic retry and error correction

### User Stories

- As a user, I want to ask questions about my data in plain English
- As a user, I want the system to automatically understand my CSV file structures
- As a user, I want safe code execution that prevents malicious operations
- As a user, I want helpful error messages if my query cannot be answered

## Flow Design

> Notes for AI:
> 1. Consider the design patterns of agent, map-reduce, rag, and workflow. Apply them if they fit.
> 2. Present a concise, high-level description of the workflow.

### Applicable Design Pattern:

1. **Workflow Pattern**: A sequential pipeline of nodes with conditional branching for error handling
2. **Agent Pattern**: LLM-driven planning and code generation based on context (schema + question)
3. **Error Correction Loop**: Automatic retry mechanism with max attempts

### Flow High-Level Design:

1. **LoadData**: Scans CSV folder and loads all CSV files as pandas DataFrames
2. **SchemaInference**: Extracts column names from each DataFrame to build schema
3. **ClarifyQuery**: Validates the user's question against available schema
4. **Planner**: Uses LLM to create analysis plan
5. **CodeGenerator**: Uses LLM to generate Python/pandas code
6. **SafetyCheck**: AST-based security check to block dangerous imports
7. **Executor**: Runs generated code in sandboxed environment
8. **ErrorFixer**: Handles execution errors with retry logic (max 3 attempts)
9. **Visualizer**: Creates visualizations for DataFrame results
10. **ResponseFormatter**: Formats final answer for user

```mermaid
flowchart TD
    load[LoadData] --> schema[SchemaInference]
    schema --> clarify[ClarifyQuery]
    
    clarify -->|ambiguous| askUser[AskUser]
    clarify -->|clear| plan[Planner]
    
    plan --> codeGen[CodeGenerator]
    codeGen --> safety[SafetyCheck]
    
    safety -->|unsafe| codeGen
    safety -->|safe| executor[Executor]
    
    executor -->|error| fixer[ErrorFixer]
    fixer -->|fix| codeGen
    fixer -->|give_up| formatter[ResponseFormatter]
    executor -->|success| viz[Visualizer]
    
    viz --> formatter
```

## Utility Functions

> Notes for AI:
> 1. Understand the utility function definition thoroughly by reviewing the doc.
> 2. Include only the necessary utility functions, based on nodes in the flow.

1. **Call LLM** (`utils/call_llm.py`)
   - *Input*: prompt (str)
   - *Output*: response (str)
   - *Config*: Uses OpenRouter API with configurable model via environment variables
   - *Environment Variables*:
     - `OPENROUTER_API_KEY`: API key for OpenRouter
     - `LLM_MODEL`: Model to use (default: meta-llama/llama-3.3-70b-instruct)
   - Used by: Planner, CodeGenerator nodes

## Node Design

### Shared Store

> Notes for AI: Try to minimize data redundancy

The shared store structure is organized as follows:

```python
shared = {
    # Input
    "question": str,              # User's natural language question
    
    # Data loaded from CSV files
    "dfs": dict,                  # {"table_name": pd.DataFrame, ...}
    "schema_str": str,            # Human-readable schema description
    
    # Planning and code
    "plan_steps": str,            # LLM-generated analysis plan
    "code_snippet": str,          # Generated Python code
    
    # Execution state
    "exec_result": Any,           # Result from successful code execution
    "exec_error": str,            # Error message if execution failed
    "retry_count": int,           # Number of retry attempts (max 3)
    
    # Output
    "chart_path": str,            # Path to generated visualization (if any)
    "final_text": str,            # Final formatted response to user
}
```

### Node Steps

> Notes for AI: Carefully decide whether to use Batch/Async Node/Flow.

1. **LoadData**
   - *Purpose*: Load all CSV files from the CSV directory
   - *Type*: Regular Node
   - *Steps*:
     - *prep*: Return path to CSV directory
     - *exec*: Scan directory and read each .csv file into a DataFrame
     - *post*: Store dict of DataFrames in `shared["dfs"]`

2. **SchemaInference**
   - *Purpose*: Extract column information from all DataFrames
   - *Type*: Regular Node
   - *Steps*:
     - *prep*: Read `dfs` from shared store
     - *exec*: Build schema string with table names and column lists
     - *post*: Store schema in `shared["schema_str"]`

3. **ClarifyQuery**
   - *Purpose*: Validate if user question can be answered with available data
   - *Type*: Regular Node
   - *Steps*:
     - *prep*: Read question and schema from shared
     - *exec*: Check for ambiguous references
     - *post*: Return "clear" or "ambiguous" action

4. **AskUser**
   - *Purpose*: Terminal node for ambiguous queries
   - *Type*: Regular Node (terminal)

5. **Planner**
   - *Purpose*: Generate analysis plan using LLM
   - *Type*: Regular Node
   - *Steps*:
     - *prep*: Read question and schema
     - *exec*: Call LLM with planning prompt
     - *post*: Store plan in `shared["plan_steps"]`

6. **CodeGenerator**
   - *Purpose*: Generate Python/pandas code using LLM
   - *Type*: Regular Node
   - *Steps*:
     - *prep*: Read plan, schema, question, and any previous errors
     - *exec*: Call LLM to generate code (or fix code if error exists)
     - *post*: Store code in `shared["code_snippet"]`

7. **SafetyCheck**
   - *Purpose*: AST-based security validation
   - *Type*: Regular Node
   - *Steps*:
     - *prep*: Read code snippet
     - *exec*: Parse AST and check for forbidden imports (os, subprocess, sys, shutil)
     - *post*: Return "safe" or "unsafe" action

8. **Executor**
   - *Purpose*: Execute generated code in sandboxed environment
   - *Type*: Regular Node
   - *Steps*:
     - *prep*: Read code and dataframes
     - *exec*: Execute code with only `dfs` and `pd` in scope, extract `final_result`
     - *post*: Store result or error, return "success" or "error" action

9. **ErrorFixer**
   - *Purpose*: Handle execution errors with retry limit
   - *Type*: Regular Node
   - *Config*: MAX_RETRIES = 3
   - *Steps*:
     - *prep*: Read error, code, and retry count
     - *exec*: Check if max retries exceeded
     - *post*: Increment retry count, return "fix" or "give_up" action

10. **Visualizer**
    - *Purpose*: Create charts for DataFrame results
    - *Type*: Regular Node
    - *Steps*:
      - *prep*: Read execution result
      - *exec*: Generate plot if result is a DataFrame
      - *post*: Store chart path in `shared["chart_path"]`

11. **ResponseFormatter**
    - *Purpose*: Format final answer for user
    - *Type*: Regular Node
    - *Steps*:
      - *prep*: Read execution result (or None if from give_up path)
      - *exec*: Format result as human-readable string
      - *post*: Store in `shared["final_text"]`

## File Structure

```
project/
├── main.py              # Entry point - runs the flow
├── nodes.py             # All 11 node class definitions
├── flow.py              # Flow creation and node wiring
├── utils/
│   ├── __init__.py
│   └── call_llm.py      # OpenRouter LLM wrapper
├── CSV/                 # User's CSV data files go here
├── docs/
│   └── design.md        # This design document
├── .env.example         # Environment variable template
└── requirements.txt     # Python dependencies
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENROUTER_API_KEY` | OpenRouter API key | (required) |
| `LLM_MODEL` | Model to use | `meta-llama/llama-3.3-70b-instruct` |

### Dependencies

- `pocketflow` - Core flow framework
- `openai` - OpenAI-compatible client for OpenRouter
- `pandas` - Data manipulation

## Usage

1. Place CSV files in the `CSV/` directory
2. Edit the question in `main.py`
3. Run: `python main.py`
