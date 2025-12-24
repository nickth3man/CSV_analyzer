# NBA Expert - Data Analyst Agent

Welcome to the NBA Expert documentation! This is an LLM-powered data analyst that answers natural language questions about your CSV data.

## Overview

NBA Expert is built with [Pocket Flow](https://github.com/The-Pocket/PocketFlow), a minimalist 100-line LLM framework. It provides an intelligent agent that can:

- **Understand Natural Language**: Ask questions about your data in plain English
- **Analyze Multiple Tables**: Automatically discovers relationships across CSV files
- **Resolve Entities**: Finds and matches entities (people, teams, etc.) across tables
- **Provide Deep Insights**: Statistical comparisons and insights, not just simple lookups
- **Execute Code Safely**: Sandboxed Python execution with safety checks
- **Learn Patterns**: Remembers successful patterns for future queries
- **Report Honestly**: Clearly reports when data is missing or incomplete

## Quick Links

- [Installation Guide](getting-started/installation.md)
- [Quick Start Tutorial](getting-started/quickstart.md)
- [Architecture Overview](architecture/overview.md)
- [API Reference](api/nodes.md)

## Features

### Natural Language Queries
Ask questions about your data in plain English:
```
"Compare LeBron James and Tracy McGrady"
"Which team has the most draft picks?"
"Show me the top 10 players by games played"
```

### Multi-Table Analysis
The agent automatically discovers relationships across CSV files, allowing you to query across multiple data sources seamlessly.

### Safe Code Execution
All code is executed in a sandboxed environment with comprehensive safety checks to prevent malicious operations.

### Learning System
The agent remembers successful patterns and entity mappings, improving performance over time.

## Architecture

The agent uses a 17-node pipeline built with PocketFlow:

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

Learn more in the [Architecture Overview](architecture/overview.md).

## Getting Started

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up your API key**
   ```bash
   export OPENROUTER_API_KEY="your-key-here"
   ```

3. **Run the application**
   ```bash
   chainlit run app.py
   ```

For detailed instructions, see the [Installation Guide](getting-started/installation.md).

## Development

To contribute to NBA Expert:

1. **Install development dependencies**
   ```bash
   make install-dev
   ```

2. **Run tests**
   ```bash
   make test
   ```

3. **Check code quality**
   ```bash
   make ci
   ```

See the [Development Setup](development/setup.md) for more details.

## License

This project demonstrates [Agentic Coding](https://the-pocket.github.io/PocketFlow/guide.html) - a collaborative workflow where humans design at a high level and AI agents handle implementation.
