---
layout: default
title: "Home"
nav_order: 1
---

# NBA Expert - Data Analyst Agent

NBA Expert is an LLM-powered analyst that answers natural language questions about NBA data stored in DuckDB. It combines a Chainlit UI with a PocketFlow pipeline for table selection, SQL generation, validation, and analysis.

## Quick Start

```bash
# Install dependencies
uv sync

# Run the Chainlit web app
uv run chainlit run src/frontend/app.py
```

Then open http://localhost:8000 in your browser.

## Data Locations

- DuckDB database: `src/backend/data/nba.duckdb`
- Raw CSVs: `src/backend/data/raw/csv/`
- Knowledge store: `src/backend/data/json/knowledge_store.json`

## Key Docs

- Design: `docs/design.md`
- Data dictionary: `docs/data_dictionary.md`
- Roadmap: `docs/roadmap.md`
- Dev setup: `docs/development/setup.md`
- Tooling: `docs/development/tools.md`
