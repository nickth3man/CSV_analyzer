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

1. **Add your API key**: Set the `OPENROUTER_API_KEY` environment variable, or enter it in the Settings panel
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

## CLI Usage

The CLI provides an interactive interface for data analysis with support for ambiguous query resolution.

### Running the CLI

```bash
# Run with default question
python main.py

# Run with a custom question
python main.py "What are the top 10 teams by wins?"

# Run with a question requiring clarification
python main.py "Show me the stats for that player"
```

### CLI Session Example

```
$ python main.py "Show me stats for the player_score column"
--- Starting Analyst Agent ---
User Question: Show me stats for the player_score column

Loaded 3 dataframes.
Schema inferred:
Table 'players': [id, first_name, last_name, team_id]
Table 'teams': [id, name, city]
Table 'stats': [player_id, games, points, rebounds]

⚠️  Your query references unknown columns or tables: ['player_score']. Please clarify or check available schema.

Please provide a clarified question (or type 'quit' to exit):
> Show me the points column from the stats table

🔄 Re-analyzing with clarified question: Show me the points column from the stats table
[... analysis continues ...]
```

### Ambiguity Resolution Flow

When the CLI detects an ambiguous query:

1. **Detection**: The system checks if referenced columns/tables exist in the schema
2. **Prompt**: If ambiguous, you'll see a warning and be prompted to clarify
3. **Options**:
   - Enter a clarified question to re-run the analysis
   - Type `quit`, `exit`, or `q` to end the session
4. **Re-entry**: Clarified questions re-enter the flow at the EntityResolver node

## File Structure

```
├── app.py                      # Chainlit web interface (adds src/ to PYTHONPATH)
├── src/
│   ├── backend/                # Backend logic and utilities
│   │   ├── flow.py             # Flow creation and connections
│   │   ├── main.py             # CLI entry point
│   │   ├── config.py           # Shared configuration constants
│   │   ├── nodes/              # Modular node definitions
│   │   └── utils/              # Backend utilities (LLM wrappers, knowledge store, etc.)
│   └── frontend/               # Chainlit frontend components
│       ├── __init__.py         # Module initialization
│       ├── config.py           # Configuration and constants
│       ├── cache.py            # Dataframe caching
│       ├── data_utils.py       # Data loading and schema utilities
│       ├── knowledge_utils.py  # Knowledge store utilities
│       ├── commands.py         # Command handling
│       ├── actions.py          # Action callbacks
│       ├── steps.py            # Analysis pipeline steps
│       └── handlers.py         # Main event handlers
├── data/
│   ├── raw/
│   │   └── csv/                # Raw CSV data files
│   └── nba.duckdb              # DuckDB database (generated)
├── scripts/                    # Database and data processing scripts
│   ├── convert_csvs.py         # CSV to DuckDB ingestion
│   ├── normalize_db.py         # Data type normalization
│   ├── check_integrity.py      # Database integrity checks
│   ├── populate/               # NBA API data population package
│   │   ├── cli.py              # Unified CLI for all population commands
│   │   ├── api_client.py       # Enhanced NBA API client with rate limiting
│   │   ├── base.py             # Base populator class with common functionality
│   │   ├── populate_player_game_stats_v2.py  # Bulk player game stats
│   │   ├── populate_player_season_stats.py   # Aggregated season stats
│   │   └── populate_play_by_play.py          # Play-by-play data
│   └── ...                     # Other utility scripts
├── docs/
│   └── design.md               # High-level design documentation
└── requirements.txt            # Project dependencies
```

## Database Population

The project includes a comprehensive NBA data population system that fetches data from the NBA API and stores it in a DuckDB database.

### Quick Population

```bash
# Run the full population pipeline (init + CSV load + normalize + API fetch + aggregation)
python -m scripts.populate.cli all --skip-api  # Skip API fetching for faster setup

# Or run individual steps:
python -m scripts.populate.cli init            # Initialize database schema
python -m scripts.populate.cli load-csv        # Load CSV files into database
python -m scripts.populate.cli normalize       # Normalize data types
python -m scripts.populate.cli player-games --seasons 2025-26 2024-25  # Fetch from NBA API
python -m scripts.populate.cli season-stats    # Create aggregated stats
```

### Population CLI Commands

| Command | Description |
|---------|-------------|
| `init` | Initialize database schema (creates tables) |
| `info` | Show database information and table row counts |
| `load-csv` | Load data from CSV files in `data/raw/csv/` |
| `normalize` | Normalize data types and create silver tables |
| `player-games` | Fetch player game stats from NBA API (bulk endpoint) |
| `player-games-legacy` | Fetch player game stats (per-player endpoint, slower) |
| `play-by-play` | Fetch play-by-play data for games |
| `season-stats` | Create aggregated player season statistics |
| `all` | Run full population pipeline |

### Database Contents

The database is **generated** (not tracked in git) and populated from:
1. **CSV files** in `data/raw/csv/` - historical data included in the repo
2. **NBA API** - live game stats fetched on demand

After running `load-csv`, the database contains static reference data:

| Table | Source | Description |
|-------|--------|-------------|
| `player` | CSV | Player master data (~4,800 players) |
| `team` | CSV | NBA team information (30 teams) |
| `game` | CSV | Historical game records (~65,000 games) |
| `common_player_info` | CSV | Detailed player biographical info |
| `draft_history` | CSV | NBA draft history |

After running `player-games`, additional data is fetched from NBA API:

| Table | Source | Description |
|-------|--------|-------------|
| `player_game_stats` | API | Player box scores per game (~27,000/season) |
| `player_season_stats` | Aggregated | Season averages (generated from player_game_stats) |

Use `python -m scripts.populate.cli info` to see current database contents.

### Supported Seasons

The system supports NBA seasons from 1996-97 to present (2025-26). Each season typically contains:
- ~1,230 regular season games (~26,000 player game records)
- ~80-90 playoff games (~1,700 player game records)

### Incremental Updates

The population system supports incremental updates:
- Progress is tracked per season/season-type combination in `.nba_cache/`
- Already completed seasons are automatically skipped
- Use `--reset` flag to force re-population

```bash
# Fetch only new seasons (skips already completed)
python -m scripts.populate.cli player-games --seasons 2025-26

# Force re-fetch all specified seasons
python -m scripts.populate.cli player-games --seasons 2025-26 --reset

# Fetch specific season types only
python -m scripts.populate.cli player-games --seasons 2024-25 --regular-only
python -m scripts.populate.cli player-games --seasons 2024-25 --playoffs-only

# Fetch multiple seasons at once
python -m scripts.populate.cli player-games --seasons 2025-26 2024-25 2023-24
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NBA_DB_PATH` | Path to DuckDB database | `data/nba.duckdb` |
| `NBA_API_TIMEOUT` | API request timeout (seconds) | `30` |
| `NBA_API_DELAY` | Delay between API requests (seconds) | `0.6` |
| `NBA_API_PROXY` | Proxy URL for API requests | None |

## Dependencies

- `pocketflow` - Core LLM framework
- `openai` - OpenAI-compatible API client
- `chainlit` - Web interface
- `pandas` - Data manipulation
- `matplotlib` - Visualizations
- `requests` - HTTP requests for model fetching
- `duckdb` - High-performance analytical database
- `nba_api` - NBA Stats API client

## Development

### Quick Start for Developers

This project uses [uv](https://docs.astral.sh/uv/) for fast, reliable Python package management.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install all dependencies (creates .venv automatically)
uv sync

# Run the application
uv run chainlit run app.py

# Run tests
uv run pytest tests/

# Run linting
uv run ruff check .
uv run mypy src/

# Auto-fix linting issues
uv run ruff check --fix .
uv run ruff format .
```

### Alternative: Using Make

```bash
# Install development dependencies
make install-dev

# Run all quality checks
make ci

# See all available commands
make help
```

### Development Tools

This project uses a comprehensive set of modern development tools:

- **Package Management**: [uv](https://docs.astral.sh/uv/) - Fast Python package installer and resolver
- **Code Quality**: Ruff, mypy
- **Testing**: pytest with coverage
- **Security**: Bandit, pip-audit
- **Documentation**: MkDocs, interrogate
- **Workflow**: pre-commit, commitizen, Makefile
- **CI/CD**: GitHub Actions, Dependabot

See [DEVELOPMENT_TOOLS.md](DEVELOPMENT_TOOLS.md) for comprehensive documentation on all development tools and workflows.

### Common Commands with uv

```bash
uv run pytest tests/           # Run tests
uv run ruff check .            # Check code quality
uv run ruff check --fix .      # Auto-fix linting issues
uv run ruff format .           # Format code
uv run mypy src/               # Type checking
uv run pip-audit               # Security audit
uv run mkdocs serve            # View documentation locally
```

### Common Commands with Make

```bash
make test          # Run tests
make lint          # Check code quality
make lint-fix      # Auto-fix linting issues
make security      # Run security scans
make docs-serve    # View documentation locally
make commit        # Make a conventional commit
```

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

---

## Documentation TODOs

<!--
TODO (Documentation): Add visual architecture diagram
The current text-based architecture diagram (lines 56-68) should be replaced
with a proper visual diagram. Options:
  1. Mermaid diagram in README (GitHub renders these)
  2. PNG/SVG diagram created with draw.io or similar
  3. Interactive diagram using D2 or Structurizr

TODO (Documentation): Add API reference documentation
Missing formal API docs for:
  - Backend utilities (call_llm, nba_api_client, knowledge_store)
  - Node interfaces and expected inputs/outputs
  - Frontend handlers and commands
Use MkDocs with mkdocstrings for auto-generation from docstrings.

TODO (Documentation): Complete MkDocs site
mkdocs.yml is configured but pages are incomplete:
  - docs/development/setup.md needs expansion
  - Missing: docs/api/index.md
  - Missing: docs/architecture/nodes.md
  - Missing: docs/architecture/data-flow.md
Run: mkdocs serve to preview locally

TODO (Documentation): Add deployment guide
Missing production deployment documentation:
  - Docker deployment (Dockerfile, docker-compose.yml)
  - Cloud deployment (AWS, GCP, Azure)
  - Environment variable configuration
  - Scaling considerations
  - Monitoring and logging setup

TODO (Documentation): Add troubleshooting guide
Common issues and solutions:
  - API key errors
  - NBA API rate limiting
  - CSV parsing errors
  - Memory issues with large datasets
  - Timeout errors

TODO (Documentation): Add contribution guidelines
CONTRIBUTING.md should include:
  - Development setup
  - Code style guidelines
  - Pull request process
  - Testing requirements
  - Commit message conventions (commitizen configured but not documented)
-->
