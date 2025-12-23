<h1 align="center">Data Analyst Agent</h1>

<p align="center">
  <a href="https://github.com/The-Pocket/PocketFlow" target="_blank">
    <img 
      src="./assets/banner.png" width="800"
    />
  </a>
</p>

An LLM-powered data analyst that answers natural language questions about your CSV data. Built with [Pocket Flow](https://github.com/The-Pocket/PocketFlow), a minimalist 100-line LLM framework.

## Features

- **Natural Language Queries**: Ask questions about your data in plain English
- **Multi-Table Analysis**: Automatically discovers relationships across CSV files
- **Entity Resolution**: Finds and matches entities (people, teams, etc.) across tables
- **Deep Analysis**: Statistical comparisons and insights, not just simple lookups
- **Safe Code Execution**: Sandboxed Python execution with safety checks
- **Learning System**: Remembers successful patterns for future queries
- **Honest Reporting**: Clearly reports when data is missing or incomplete

## Quick Start

1. **Add your API key**: Open the app and expand "Settings" to enter your OpenRouter API key
2. **Upload data**: Click "📁 Upload CSV" or type `/upload` to add CSV files (or use the pre-loaded NBA data)
3. **Ask questions**: Type a question like "Compare LeBron James and Tracy McGrady"
4. **Get insights**: The agent analyzes your data and provides detailed responses

## Web Interface

The Chainlit web interface provides:

### Features
- **Chat Interface**: Ask questions and view responses with real-time status updates
- **Quick Actions**: Buttons for common tasks (Upload CSV, List Tables, View Schema, Help)
- **Commands**: Type commands like `/upload`, `/tables`, `/schema`, `/knowledge`, `/help`
- **File Management**: Upload and preview CSV files
- **Knowledge Store**: View learned patterns and entity mappings with `/knowledge`

### Settings
- **API Key**: Enter your OpenRouter API key
- **Model Selection**: Choose from available LLM models (fetched live from OpenRouter)

## Example Questions

- "Compare the careers of LeBron James and Tracy McGrady"
- "Which team has the most draft picks?"
- "Show me the top 10 players by games played"
- "What are the statistics for Chicago Bulls?"
- "Find all players drafted in 2003"

## Architecture

The agent uses a 17-node pipeline:

```
LoadData → SchemaInference → DataProfiler → ClarifyQuery
                                                ↓
EntityResolver → SearchExpander → ContextAggregator → Planner
                                                        ↓
                            CodeGenerator ←→ SafetyCheck → Executor
                                   ↑                         ↓
                              ErrorFixer ←─────────── ResultValidator
                                                          ↓
                                    DeepAnalyzer → Visualizer → ResponseSynthesizer
```

## File Structure

```
├── chainlit_app.py        # Chainlit web interface (main entry point)
├── main.py                # CLI entry point
├── nodes.py               # Node definitions (17 nodes)
├── flow.py                # Flow creation and connections
├── utils/
│   ├── call_llm.py        # LLM API calls
│   ├── call_llm_streaming.py  # Streaming LLM calls
│   └── knowledge_store.py # Persistent learning storage
├── CSV/                   # Data directory for CSV files
├── docs/
│   └── design.md          # High-level design documentation
└── requirements.txt       # Project dependencies
```

## Dependencies

- `pocketflow` - Core LLM framework
- `openai` - OpenAI-compatible API client
- `chainlit` - Web interface
- `pandas` - Data manipulation
- `matplotlib` - Visualizations
- `requests` - HTTP requests for model fetching

## Agentic Coding

This project demonstrates [Agentic Coding](https://the-pocket.github.io/PocketFlow/guide.html) - a collaborative workflow where humans design at a high level and AI agents handle implementation.

- Check out the [Agentic Coding Guidance](https://the-pocket.github.io/PocketFlow/guide.html)
- Check out the [YouTube Tutorial](https://www.youtube.com/@ZacharyLLM?sub_confirmation=1)

### AI Coding Assistant Rules

- [.cursorrules](.cursorrules) for Cursor AI
- [.clinerules](.clinerules) for Cline
- [.windsurfrules](.windsurfrules) for Windsurf
- [.goosehints](.goosehints) for Goose
- Configuration in [.github](.github) for GitHub Copilot
- [CLAUDE.md](CLAUDE.md) for Claude Code
- [GEMINI.md](GEMINI.md) for Gemini
