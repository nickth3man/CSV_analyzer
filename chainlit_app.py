import chainlit as cl
from chainlit.input_widget import Select, TextInput
import pandas as pd
import os
import shutil
import requests
from flow import create_analyst_flow
from utils.knowledge_store import knowledge_store
from utils.file_sanitizer import sanitize_csv_filename

DEFAULT_MODELS = [
    "meta-llama/llama-3.3-70b-instruct",
    "anthropic/claude-3.5-sonnet",
    "openai/gpt-4o",
    "google/gemini-2.0-flash-001",
    "deepseek/deepseek-chat-v3-0324"
]

EXAMPLE_QUESTIONS = [
    "Compare the careers of LeBron James and Tracy McGrady",
    "Which team has the most draft picks?",
    "Show me the top 10 players by games played",
    "What are the statistics for Chicago Bulls?",
    "Find all players drafted in 2003"
]

HELP_TEXT = """## How to Use the Data Analyst Agent

### Getting Started
1. **Upload your data**: Click the 📁 button or type `/upload` to upload CSV files
2. **Ask questions**: Type your question in plain English
3. **Get insights**: The agent will analyze your data and provide detailed responses

### Commands
- `/upload` - Upload CSV files
- `/tables` - List loaded tables
- `/preview <table_name>` - Preview a table
- `/delete <table_name>` - Delete a table
- `/schema` - View data schema
- `/schema <table_name>` - View schema for a specific table
- `/profile` - View data profile summary
- `/knowledge` - View learned patterns
- `/clear_knowledge` - Clear learned patterns
- `/help` - Show this help

### Example Questions
- "Compare the careers of LeBron James and Tracy McGrady"
- "Which team has the most draft picks?"
- "Show me the top 10 players by games played"

### Tips for Better Results
- **Be specific**: Instead of "show me data", ask "What are the top 10 players by points scored?"
- **Name entities clearly**: "Compare LeBron James and Kobe Bryant" works better than "compare the best players"
- **Use comparisons**: The agent excels at comparing entities across tables
"""

def fetch_openrouter_models(api_key=None):
    if not api_key:
        api_key = os.environ.get("OPENROUTER_API_KEY", "")
    
    if not api_key:
        return DEFAULT_MODELS
    
    try:
        response = requests.get(
            "https://openrouter.ai/api/v1/models",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            models = []
            for model in data.get("data", []):
                model_id = model.get("id", "")
                if model_id:
                    models.append(model_id)
            models.sort()
            return models[:50] if models else DEFAULT_MODELS
    except (requests.RequestException, ValueError) as e:
        print(f"Warning: Could not fetch OpenRouter models: {e}")
        pass
    
    return DEFAULT_MODELS

class DataFrameCache:
    """
    Cache for loaded dataframes to avoid re-reading CSV files on each command.
    Implements cache invalidation based on directory modification time.
    """
    def __init__(self, csv_dir="CSV"):
        self.csv_dir = csv_dir
        self._cache = {}
        self._last_mtime = None
        self._file_mtimes = {}
    
    def _get_dir_state(self):
        """Get the current state of the CSV directory (files and their mtimes)."""
        if not os.path.exists(self.csv_dir):
            return None, {}
        
        dir_mtime = os.path.getmtime(self.csv_dir)
        file_mtimes = {}
        for filename in os.listdir(self.csv_dir):
            if filename.endswith(".csv"):
                filepath = os.path.join(self.csv_dir, filename)
                try:
                    file_mtimes[filename] = os.path.getmtime(filepath)
                except OSError:
                    pass
        return dir_mtime, file_mtimes
    
    def _is_cache_valid(self):
        """Check if the cache is still valid based on directory state."""
        dir_mtime, file_mtimes = self._get_dir_state()
        
        # Invalid if directory doesn't exist or was modified
        if dir_mtime is None:
            return False
        if self._last_mtime is None or dir_mtime != self._last_mtime:
            return False
        
        # Invalid if any file was modified or files changed
        if set(file_mtimes.keys()) != set(self._file_mtimes.keys()):
            return False
        for filename, mtime in file_mtimes.items():
            if self._file_mtimes.get(filename) != mtime:
                return False
        
        return True
    
    def invalidate(self):
        """Force cache invalidation (e.g., after upload/delete)."""
        self._cache = {}
        self._last_mtime = None
        self._file_mtimes = {}
    
    def get_dataframes(self):
        """Get cached dataframes, reloading if cache is invalid."""
        if self._is_cache_valid() and self._cache:
            return self._cache
        
        # Reload dataframes
        dfs = {}
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)
        
        for filename in os.listdir(self.csv_dir):
            if filename.endswith(".csv"):
                filepath = os.path.join(self.csv_dir, filename)
                try:
                    table_name = filename.replace(".csv", "")
                    dfs[table_name] = pd.read_csv(filepath)
                except (pd.errors.ParserError, UnicodeDecodeError) as e:
                    print(f"Warning: Could not parse {filename}: {e}")
                except Exception as e:
                    print(f"Warning: Unexpected error loading {filename}: {e}")
        
        # Update cache state
        self._cache = dfs
        self._last_mtime, self._file_mtimes = self._get_dir_state()
        
        return dfs


# Global dataframe cache instance
_df_cache = DataFrameCache()


def get_csv_files():
    csv_dir = "CSV"
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    return [f.replace(".csv", "") for f in os.listdir(csv_dir) if f.endswith('.csv')]

def load_dataframes():
    """Load dataframes using cache to avoid redundant disk reads."""
    return _df_cache.get_dataframes()

def invalidate_dataframe_cache():
    """Invalidate the dataframe cache after file changes."""
    _df_cache.invalidate()

def get_schema_info():
    dfs = load_dataframes()
    if not dfs:
        return "No CSV files loaded. Upload some data to get started!"
    
    schema_lines = []
    # TODO: Expand schema summaries to include Excel/JSON/DB metadata once additional formats are supported.
    for name, df in dfs.items():
        cols = ", ".join(df.columns[:10])
        if len(df.columns) > 10:
            cols += f"... (+{len(df.columns) - 10} more)"
        schema_lines.append(f"**{name}** ({len(df)} rows, {len(df.columns)} columns)\n  Columns: {cols}")
    return "\n\n".join(schema_lines)

def get_table_schema(table_name):
    dfs = load_dataframes()
    if not dfs:
        return "No CSV files loaded. Upload some data to get started!"
    if table_name not in dfs:
        return f"Table '{table_name}' not found."

    df = dfs[table_name]
    cols = ", ".join(df.columns)
    return f"**{table_name}** ({len(df)} rows, {len(df.columns)} columns)\n  Columns: {cols}"

def get_data_profile():
    dfs = load_dataframes()
    if not dfs:
        return "No data loaded."
    
    profile_text = []
    for name, df in dfs.items():
        name_cols = [c for c in df.columns if any(x in c.lower() for x in ['name', 'first', 'last', 'player', 'team'])]
        id_cols = [c for c in df.columns if 'id' in c.lower()]
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        
        profile_text.append(f"""### {name}
- Rows: {len(df):,}
- Columns: {len(df.columns)}
- Key columns: {', '.join(name_cols[:5]) if name_cols else 'None identified'}
- ID columns: {', '.join(id_cols[:5]) if id_cols else 'None identified'}
- Numeric columns: {len(numeric_cols)}
""")
    return "\n".join(profile_text)

def preview_table(table_name):
    dfs = load_dataframes()
    if table_name in dfs:
        df = dfs[table_name]
        return df.head(20).to_markdown(index=False)
    return "Table not found"

def get_knowledge_store_data():
    data = knowledge_store.data
    
    output = "## Entity Mappings\n"
    if data.get("entity_mappings"):
        for entity, tables in data["entity_mappings"].items():
            output += f"**{entity}**\n"
            for table, cols in tables.items():
                output += f"  - {table}: {', '.join(cols)}\n"
    else:
        output += "No entity mappings yet.\n"
    
    output += "\n## Successful Query Patterns\n"
    if data.get("successful_patterns"):
        for qtype, patterns in data["successful_patterns"].items():
            output += f"**{qtype}**: {len(patterns)} patterns\n"
    else:
        output += "No patterns learned yet.\n"
    
    output += "\n## Join Patterns\n"
    if data.get("join_patterns"):
        for pattern in data["join_patterns"][:5]:
            output += f"- Tables: {pattern['tables']}, Keys: {pattern['keys']}\n"
    else:
        output += "No join patterns discovered yet.\n"
    
    return output

def clear_knowledge_store():
    knowledge_store.data = {
        "entity_mappings": {},
        "successful_patterns": {},
        "column_hints": {},
        "join_patterns": []
    }
    knowledge_store.save()
    return "Knowledge store cleared!"


@cl.on_chat_start
async def on_chat_start():
    models = fetch_openrouter_models()
    
    settings = await cl.ChatSettings(
        [
            TextInput(
                id="api_key",
                label="OpenRouter API Key",
                initial=os.environ.get("OPENROUTER_API_KEY", ""),
                placeholder="sk-or-v1-..."
            ),
            Select(
                id="model",
                label="LLM Model",
                values=models,
                initial_index=0
            ),
        ]
    ).send()
    
    cl.user_session.set("settings", settings)
    cl.user_session.set("chat_history", [])
    
    tables = get_csv_files()
    table_info = f"**{len(tables)} tables loaded**: {', '.join(tables)}" if tables else "No data loaded yet"
    
    actions = [
        cl.Action(name="upload_csv", payload={"action": "upload"}, label="📁 Upload CSV", description="Upload CSV files"),
        cl.Action(name="list_tables", payload={"action": "tables"}, label="📋 Tables", description="List loaded tables"),
        cl.Action(name="view_schema", payload={"action": "schema"}, label="📊 Schema", description="View data schema"),
        cl.Action(name="view_profile", payload={"action": "profile"}, label="🧾 Profile", description="View data profile"),
        cl.Action(name="show_help", payload={"action": "help"}, label="❓ Help", description="Show help"),
    ]
    
    welcome_msg = f"""# Data Analyst Agent

Ask questions about your data in plain English. The agent will analyze your CSV files and provide insights.

{table_info}

**Quick Actions:** Use the buttons below or type commands like `/upload`, `/tables`, `/help`

**Example questions:**
"""
    for q in EXAMPLE_QUESTIONS[:3]:
        welcome_msg += f"\n- {q}"
    
    await cl.Message(
        content=welcome_msg,
        actions=actions
    ).send()


@cl.on_settings_update
async def on_settings_update(settings):
    cl.user_session.set("settings", settings)
    
    api_key = settings.get("api_key", "")
    if api_key:
        os.environ["OPENROUTER_API_KEY"] = api_key
    
    model = settings.get("model", "")
    if model:
        os.environ["LLM_MODEL"] = model
    
    await cl.Message(content="⚙️ Settings updated!").send()


@cl.action_callback("upload_csv")
async def on_upload_action(action: cl.Action):
    files = await cl.AskFileMessage(
        content="Please upload your CSV file(s)",
        accept=["text/csv", "application/vnd.ms-excel", ".csv"],
        max_size_mb=50,
        max_files=10
    ).send()
    
    if files:
        csv_dir = "CSV"
        os.makedirs(csv_dir, exist_ok=True)

        uploaded = []
        for file in files:
            # SECURITY: sanitize filenames to prevent path traversal
            filename = sanitize_csv_filename(file.name)
            if not filename:
                continue  # Skip invalid filenames
            dest = os.path.join(csv_dir, filename)
            shutil.copy(file.path, dest)
            uploaded.append(filename.replace('.csv', ''))

        # Invalidate cache after upload
        invalidate_dataframe_cache()
        await cl.Message(content=f"✅ Uploaded: {', '.join(uploaded)}\n\nYou can now ask questions about your data!").send()

    return "Upload complete"


@cl.action_callback("list_tables")
async def on_list_tables(action: cl.Action):
    tables = get_csv_files()
    if tables:
        content = "## Loaded Tables\n\n"
        for t in tables:
            content += f"- **{t}**\n"
        content += f"\nUse `/preview <table_name>` to preview a table."
    else:
        content = "No tables loaded. Use the 📁 button to upload CSV files."
    
    await cl.Message(content=content).send()
    return "Listed tables"


@cl.action_callback("view_schema")
async def on_view_schema(action: cl.Action):
    schema = get_schema_info()
    profile = get_data_profile()
    
    content = f"## Data Schema\n\n{schema}\n\n## Data Profile\n\n{profile}"
    await cl.Message(content=content).send()
    return "Showed schema"

@cl.action_callback("view_profile")
async def on_view_profile(action: cl.Action):
    profile = get_data_profile()
    await cl.Message(content=f"## Data Profile\n\n{profile}").send()
    return "Showed data profile"


@cl.action_callback("show_help")
async def on_show_help(action: cl.Action):
    await cl.Message(content=HELP_TEXT).send()
    return "Showed help"


async def handle_command(message_content: str) -> bool:
    content = message_content.strip().lower()
    
    if content == "/upload":
        files = await cl.AskFileMessage(
            content="Please upload your CSV file(s)",
            accept=["text/csv", "application/vnd.ms-excel", ".csv"],
            max_size_mb=50,
            max_files=10
        ).send()
        
        if files:
            csv_dir = "CSV"
            os.makedirs(csv_dir, exist_ok=True)

            uploaded = []
            for file in files:
                # SECURITY: sanitize filenames to prevent path traversal
                filename = sanitize_csv_filename(file.name)
                if not filename:
                    continue  # Skip invalid filenames
                dest = os.path.join(csv_dir, filename)
                shutil.copy(file.path, dest)
                uploaded.append(filename.replace('.csv', ''))

            # Invalidate cache after upload
            invalidate_dataframe_cache()
            await cl.Message(content=f"✅ Uploaded: {', '.join(uploaded)}").send()
        return True
    
    elif content == "/tables":
        tables = get_csv_files()
        if tables:
            msg = "## Loaded Tables\n\n" + "\n".join([f"- **{t}**" for t in tables])
        else:
            msg = "No tables loaded. Use `/upload` to add CSV files."
        await cl.Message(content=msg).send()
        return True
    
    elif content.startswith("/preview "):
        table_name = message_content[9:].strip()
        preview = preview_table(table_name)
        await cl.Message(content=f"## Preview: {table_name}\n\n{preview}").send()
        return True
    
    elif content.startswith("/delete "):
        table_name = message_content[8:].strip()
        filepath = os.path.join("CSV", f"{table_name}.csv")
        if os.path.exists(filepath):
            os.remove(filepath)
            # Invalidate cache after delete
            invalidate_dataframe_cache()
            await cl.Message(content=f"✅ Deleted table: {table_name}").send()
        else:
            await cl.Message(content=f"❌ Table not found: {table_name}").send()
        return True
    
    elif content.startswith("/schema "):
        table_name = message_content[8:].strip()
        schema = get_table_schema(table_name)
        await cl.Message(content=f"## Table Schema\n\n{schema}").send()
        return True

    elif content == "/schema":
        schema = get_schema_info()
        await cl.Message(content=f"## Data Schema\n\n{schema}").send()
        return True

    elif content == "/profile":
        profile = get_data_profile()
        await cl.Message(content=f"## Data Profile\n\n{profile}").send()
        return True
    
    elif content == "/knowledge":
        knowledge = get_knowledge_store_data()
        await cl.Message(content=f"## Knowledge Store\n\n{knowledge}").send()
        return True
    
    elif content == "/clear_knowledge":
        result = clear_knowledge_store()
        await cl.Message(content=f"✅ {result}").send()
        return True
    
    elif content == "/help":
        await cl.Message(content=HELP_TEXT).send()
        return True
    
    return False


@cl.step(type="tool", name="Loading Data")
async def step_load_data():
    dfs = load_dataframes()
    return f"Loaded {len(dfs)} tables"


@cl.step(type="tool", name="Analyzing Schema")
async def step_schema():
    return get_schema_info()


@cl.step(type="tool", name="Running Analysis")
async def step_run_analysis(question: str, settings: dict):
    api_key = settings.get("api_key", "")
    model = settings.get("model", "")
    
    if api_key:
        os.environ["OPENROUTER_API_KEY"] = api_key
    if model:
        os.environ["LLM_MODEL"] = model
    
    dfs = load_dataframes()
    if not dfs:
        return None, "No CSV files loaded. Please upload some data first.", None
    
    shared = {
        "question": question,
        "retry_count": 0,
        "exec_error": None,
    }
    # TODO: Generate query suggestions from the loaded schema and attach them to the shared store
    # so the UI can surface suggested questions before/after answering.
    
    try:
        analyst_flow = create_analyst_flow()
        analyst_flow.run(shared)
        
        final_text = shared.get("final_text", "Analysis complete but no response was generated.")
        chart_path = shared.get("chart_path")
        
        return shared, final_text, chart_path
        
    except Exception as e:
        return None, f"An error occurred: {str(e)}", None


@cl.on_message
async def on_message(message: cl.Message):
    if message.elements:
        csv_dir = "CSV"
        os.makedirs(csv_dir, exist_ok=True)

        uploaded = []
        for element in message.elements:
            if hasattr(element, 'path') and element.path:
                # SECURITY: sanitize filenames to prevent path traversal
                raw_name = element.name if hasattr(element, 'name') else element.path
                filename = sanitize_csv_filename(raw_name)
                if not filename:
                    continue  # Skip invalid filenames
                dest = os.path.join(csv_dir, filename)
                shutil.copy(element.path, dest)
                uploaded.append(filename.replace('.csv', ''))

        if uploaded:
            # Invalidate cache after upload
            invalidate_dataframe_cache()
            await cl.Message(content=f"✅ Uploaded: {', '.join(uploaded)}\n\nYou can now ask questions about your data!").send()
            return
    
    if await handle_command(message.content):
        return
    
    question = message.content.strip()
    if not question:
        return
    
    settings = cl.user_session.get("settings", {})
    api_key = settings.get("api_key", os.environ.get("OPENROUTER_API_KEY", ""))

    if not api_key or len(api_key.strip()) == 0:
        await cl.Message(content="⚠️ Please set your OpenRouter API key in Settings (gear icon) first.").send()
        return

    # Basic API key format validation
    if not api_key.startswith("sk-or-"):
        await cl.Message(content="⚠️ Invalid API key format. OpenRouter keys should start with 'sk-or-'.").send()
        return
    
    dfs = load_dataframes()
    if not dfs:
        await cl.Message(content="⚠️ No data loaded. Please upload CSV files first using the 📁 button or `/upload` command.").send()
        return
    
    progress_msg = await cl.Message(content="⏳ Starting analysis...").send()
    await progress_msg.update(content="📥 Loading data...")
    await step_load_data()
    await progress_msg.update(content="🧭 Analyzing schema...")
    await step_schema()
    # TODO: Display schema-driven query suggestions to the user (e.g., via a message or action buttons)
    # before running the analysis, once suggestions are generated.
    await progress_msg.update(content="🧠 Running analysis pipeline...")
    
    shared, final_text, chart_path = await step_run_analysis(question, settings)
    if shared:
        await progress_msg.update(content="✅ Analysis complete.")
    else:
        await progress_msg.update(content="⚠️ Analysis failed. See details below.")

    elements = []
    if chart_path:
        try:
            # Try to create the image element, avoiding TOCTOU race condition
            elements.append(cl.Image(path=chart_path, name="chart", display="inline"))
        except (FileNotFoundError, OSError) as e:
            print(f"Warning: Could not load chart image: {e}")

    await cl.Message(content=final_text, elements=elements).send()


if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)
