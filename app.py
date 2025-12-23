import gradio as gr
import pandas as pd
import os
import json
import shutil
import requests
from flow import create_analyst_flow
from utils.knowledge_store import knowledge_store

DEFAULT_MODELS = [
    "meta-llama/llama-3.3-70b-instruct",
    "anthropic/claude-3.5-sonnet",
    "openai/gpt-4o",
    "google/gemini-2.0-flash-001",
    "deepseek/deepseek-chat-v3-0324"
]

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
            return models if models else DEFAULT_MODELS
    except Exception:
        pass
    
    return DEFAULT_MODELS

def refresh_models(api_key):
    if api_key:
        os.environ["OPENROUTER_API_KEY"] = api_key
    models = fetch_openrouter_models(api_key)
    return gr.Dropdown(choices=models, value=models[0] if models else None)

CSS = """
.container { max-width: 100% !important; }
.mobile-friendly { padding: 8px !important; }
.chat-container { min-height: 400px; }
.progress-box {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 12px 16px;
    border-radius: 8px;
    margin-bottom: 10px;
    font-weight: 500;
}
.status-running { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }
.status-complete { background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }
.data-card {
    background: #f8f9fa;
    border-radius: 8px;
    padding: 12px;
    margin: 8px 0;
    border-left: 4px solid #667eea;
}
.table-preview { max-height: 300px; overflow-y: auto; }
footer { display: none !important; }
@media (max-width: 768px) {
    .gr-button { min-height: 44px !important; font-size: 16px !important; }
    .gr-textbox textarea { font-size: 16px !important; }
    .gr-markdown { font-size: 14px !important; }
}
"""

NODE_DESCRIPTIONS = {
    "LoadData": "Loading CSV files...",
    "SchemaInference": "Analyzing table structures...",
    "DataProfiler": "Profiling data quality...",
    "ClarifyQuery": "Understanding your question...",
    "EntityResolver": "Finding entities in data...",
    "SearchExpander": "Expanding search...",
    "ContextAggregator": "Gathering context...",
    "Planner": "Creating analysis plan...",
    "CodeGenerator": "Generating analysis code...",
    "SafetyCheck": "Checking code safety...",
    "Executor": "Running analysis...",
    "ErrorFixer": "Fixing issues...",
    "ResultValidator": "Validating results...",
    "DeepAnalyzer": "Performing deep analysis...",
    "Visualizer": "Creating visualizations...",
    "ResponseSynthesizer": "Crafting response..."
}

class ProgressTracker:
    def __init__(self):
        self.current_node = ""
        self.node_history = []
        self.messages = []
        
    def update(self, node_name, message=""):
        self.current_node = node_name
        self.node_history.append(node_name)
        if message:
            self.messages.append(message)
    
    def get_status(self):
        if not self.node_history:
            return "Ready"
        desc = NODE_DESCRIPTIONS.get(self.current_node, f"Processing {self.current_node}...")
        return f"Step {len(self.node_history)}: {desc}"
    
    def reset(self):
        self.current_node = ""
        self.node_history = []
        self.messages = []

progress_tracker = ProgressTracker()

def get_csv_files():
    csv_dir = "CSV"
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)
    return [f for f in os.listdir(csv_dir) if f.endswith('.csv')]

def load_dataframes():
    csv_dir = "CSV"
    dfs = {}
    if os.path.exists(csv_dir):
        for filename in os.listdir(csv_dir):
            if filename.endswith(".csv"):
                filepath = os.path.join(csv_dir, filename)
                try:
                    table_name = filename.replace(".csv", "")
                    dfs[table_name] = pd.read_csv(filepath)
                except Exception:
                    pass
    return dfs

def get_schema_info():
    dfs = load_dataframes()
    if not dfs:
        return "No CSV files loaded. Upload some data to get started!"
    
    schema_lines = []
    for name, df in dfs.items():
        cols = ", ".join(df.columns[:10])
        if len(df.columns) > 10:
            cols += f"... (+{len(df.columns) - 10} more)"
        schema_lines.append(f"**{name}** ({len(df)} rows, {len(df.columns)} columns)\n  Columns: {cols}")
    return "\n\n".join(schema_lines)

def get_data_profile():
    dfs = load_dataframes()
    if not dfs:
        return "No data loaded."
    
    profile_text = []
    for name, df in dfs.items():
        name_cols = [c for c in df.columns if any(x in c.lower() for x in ['name', 'first', 'last', 'player', 'team'])]
        id_cols = [c for c in df.columns if 'id' in c.lower()]
        numeric_cols = [c for c in df.columns if df[c].dtype in ['int64', 'float64']]
        
        profile_text.append(f"""### {name}
- Rows: {len(df):,}
- Columns: {len(df.columns)}
- Key columns: {', '.join(name_cols[:5]) if name_cols else 'None identified'}
- ID columns: {', '.join(id_cols[:5]) if id_cols else 'None identified'}
- Numeric columns: {len(numeric_cols)}
""")
    return "\n".join(profile_text)

def run_analysis(question, history, model_choice, api_key):
    if not question.strip():
        yield history, "Please enter a question.", None
        return
    
    if not api_key and not os.environ.get("OPENROUTER_API_KEY"):
        yield history, "Please enter your OpenRouter API key in Settings.", None
        return
    
    dfs = load_dataframes()
    if not dfs:
        yield history, "No CSV files loaded. Please upload some data first.", None
        return
    
    progress_tracker.reset()
    
    history = history or []
    history.append([question, ""])
    
    yield history, "Starting analysis...", None
    
    shared = {
        "question": question,
        "retry_count": 0,
        "exec_error": None,
    }
    
    if api_key:
        os.environ["OPENROUTER_API_KEY"] = api_key
    if model_choice:
        os.environ["LLM_MODEL"] = model_choice
    
    try:
        analyst_flow = create_analyst_flow()
        
        status_updates = [
            "Loading data...",
            "Analyzing schema...",
            "Profiling data...",
            "Resolving entities...",
            "Expanding search...",
            "Aggregating context...",
            "Planning analysis...",
            "Generating code...",
            "Executing analysis...",
            "Validating results...",
            "Creating response..."
        ]
        
        for i, status in enumerate(status_updates[:3]):
            yield history, status, None
        
        analyst_flow.run(shared)
        
        final_text = shared.get("final_text", "Analysis complete but no response was generated.")
        chart_path = shared.get("chart_path")
        
        history[-1][1] = final_text
        
        yield history, "Analysis complete!", chart_path
        
    except Exception as e:
        error_msg = f"An error occurred: {str(e)}"
        history[-1][1] = error_msg
        yield history, "Error occurred", None

def preview_table(table_name):
    dfs = load_dataframes()
    if table_name in dfs:
        df = dfs[table_name]
        return df.head(20).to_html(classes='table-preview', index=False)
    return "Table not found"

def upload_csv(files):
    if not files:
        return "No files uploaded", get_csv_files()
    
    csv_dir = "CSV"
    os.makedirs(csv_dir, exist_ok=True)
    
    uploaded = []
    for file in files:
        filename = os.path.basename(file.name)
        dest = os.path.join(csv_dir, filename)
        shutil.copy(file.name, dest)
        uploaded.append(filename)
    
    return f"Uploaded: {', '.join(uploaded)}", get_csv_files()

def delete_csv(table_name):
    if not table_name:
        return "Please select a table", get_csv_files()
    
    filepath = os.path.join("CSV", f"{table_name}.csv")
    if os.path.exists(filepath):
        os.remove(filepath)
        return f"Deleted {table_name}", get_csv_files()
    return "File not found", get_csv_files()

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
    return "Knowledge store cleared!", get_knowledge_store_data()

EXAMPLE_QUESTIONS = [
    "Compare the careers of LeBron James and Tracy McGrady",
    "Which team has the most draft picks?",
    "Show me the top 10 players by games played",
    "What are the statistics for Chicago Bulls?",
    "Find all players drafted in 2003"
]

def create_app():
    with gr.Blocks(title="Data Analyst Agent") as app:
        gr.Markdown("""
        # Data Analyst Agent
        Ask questions about your data in plain English. The agent will analyze your CSV files and provide insights.
        """)
        
        with gr.Tabs() as tabs:
            with gr.Tab("Chat", id="chat"):
                with gr.Row():
                    with gr.Column(scale=3):
                        chatbot = gr.Chatbot(
                            label="Conversation",
                            height=450,
                            render_markdown=True
                        )
                        
                        with gr.Row():
                            question_input = gr.Textbox(
                                placeholder="Ask a question about your data...",
                                label="Your Question",
                                scale=4,
                                lines=2
                            )
                            submit_btn = gr.Button("Analyze", variant="primary", scale=1)
                        
                        gr.Markdown("**Example questions:**")
                        with gr.Row():
                            for i, example in enumerate(EXAMPLE_QUESTIONS[:3]):
                                gr.Button(example, size="sm").click(
                                    lambda e=example: e,
                                    outputs=question_input
                                )
                        with gr.Row():
                            for i, example in enumerate(EXAMPLE_QUESTIONS[3:]):
                                gr.Button(example, size="sm").click(
                                    lambda e=example: e,
                                    outputs=question_input
                                )
                    
                    with gr.Column(scale=1):
                        status_display = gr.Textbox(
                            label="Status",
                            value="Ready",
                            interactive=False
                        )
                        chart_display = gr.Image(
                            label="Generated Chart",
                            visible=True,
                            height=200
                        )
                        
                        gr.Markdown("### Quick Schema")
                        schema_display = gr.Markdown(get_schema_info())
                        refresh_schema_btn = gr.Button("Refresh Schema", size="sm")
                        refresh_schema_btn.click(get_schema_info, outputs=schema_display)
                
                with gr.Accordion("Settings", open=False):
                    api_key_input = gr.Textbox(
                        label="OpenRouter API Key",
                        placeholder="Enter your OpenRouter API key (sk-or-v1-...)",
                        type="password"
                    )
                    with gr.Row():
                        model_choice = gr.Dropdown(
                            choices=fetch_openrouter_models(),
                            value="meta-llama/llama-3.3-70b-instruct",
                            label="LLM Model",
                            scale=3
                        )
                        refresh_models_btn = gr.Button("Refresh Models", size="sm", scale=1)
                    
                    refresh_models_btn.click(
                        refresh_models,
                        inputs=[api_key_input],
                        outputs=[model_choice]
                    )
                    
                    clear_chat_btn = gr.Button("Clear Chat")
                    clear_chat_btn.click(lambda: ([], "Ready", None), outputs=[chatbot, status_display, chart_display])
                
                submit_btn.click(
                    run_analysis,
                    inputs=[question_input, chatbot, model_choice, api_key_input],
                    outputs=[chatbot, status_display, chart_display]
                )
                question_input.submit(
                    run_analysis,
                    inputs=[question_input, chatbot, model_choice, api_key_input],
                    outputs=[chatbot, status_display, chart_display]
                )
            
            with gr.Tab("Data", id="data"):
                gr.Markdown("## Data Management")
                gr.Markdown("Upload, view, and manage your CSV files.")
                
                with gr.Row():
                    with gr.Column(scale=1):
                        file_upload = gr.File(
                            label="Upload CSV Files",
                            file_types=[".csv"],
                            file_count="multiple"
                        )
                        upload_status = gr.Textbox(label="Upload Status", interactive=False)
                        upload_btn = gr.Button("Upload", variant="primary")
                        
                        gr.Markdown("### Loaded Tables")
                        table_list = gr.Dropdown(
                            choices=get_csv_files(),
                            label="Select Table",
                            interactive=True
                        )
                        refresh_tables_btn = gr.Button("Refresh List", size="sm")
                        delete_btn = gr.Button("Delete Selected", variant="stop", size="sm")
                        delete_status = gr.Textbox(label="Status", interactive=False, visible=False)
                    
                    with gr.Column(scale=2):
                        table_preview = gr.HTML(
                            label="Table Preview",
                            value="<p>Select a table to preview</p>"
                        )
                
                with gr.Accordion("Data Profile", open=False):
                    profile_display = gr.Markdown(get_data_profile())
                    refresh_profile_btn = gr.Button("Refresh Profile", size="sm")
                    refresh_profile_btn.click(get_data_profile, outputs=profile_display)
                
                upload_btn.click(
                    upload_csv,
                    inputs=[file_upload],
                    outputs=[upload_status, table_list]
                )
                
                table_list.change(
                    lambda x: preview_table(x.replace(".csv", "") if x else ""),
                    inputs=[table_list],
                    outputs=[table_preview]
                )
                
                refresh_tables_btn.click(
                    lambda: gr.Dropdown(choices=get_csv_files()),
                    outputs=[table_list]
                )
                
                delete_btn.click(
                    lambda x: delete_csv(x.replace(".csv", "") if x else ""),
                    inputs=[table_list],
                    outputs=[delete_status, table_list]
                )
            
            with gr.Tab("History", id="history"):
                gr.Markdown("## Knowledge Store")
                gr.Markdown("The agent learns from previous queries. View and manage learned patterns.")
                
                knowledge_display = gr.Markdown(get_knowledge_store_data())
                
                with gr.Row():
                    refresh_knowledge_btn = gr.Button("Refresh", size="sm")
                    clear_knowledge_btn = gr.Button("Clear All", variant="stop", size="sm")
                
                clear_status = gr.Textbox(label="Status", interactive=False, visible=False)
                
                refresh_knowledge_btn.click(get_knowledge_store_data, outputs=knowledge_display)
                clear_knowledge_btn.click(
                    clear_knowledge_store,
                    outputs=[clear_status, knowledge_display]
                )
            
            with gr.Tab("Help", id="help"):
                gr.Markdown("""
                ## How to Use the Data Analyst Agent
                
                ### Getting Started
                1. **Upload your data**: Go to the "Data" tab and upload CSV files
                2. **Ask questions**: Go to the "Chat" tab and ask questions in plain English
                3. **Get insights**: The agent will analyze your data and provide detailed responses
                
                ### Tips for Better Results
                - **Be specific**: Instead of "show me data", ask "What are the top 10 players by points scored?"
                - **Name entities clearly**: "Compare LeBron James and Kobe Bryant" works better than "compare the best players"
                - **Use comparisons**: The agent excels at comparing entities across tables
                
                ### Supported Question Types
                - **Lookups**: "Show me information about the Lakers"
                - **Comparisons**: "Compare Team A and Team B"
                - **Aggregations**: "What is the average score per game?"
                - **Rankings**: "Who are the top 5 scorers?"
                - **Filters**: "Find all games in 2023"
                
                ### Technical Details
                - The agent uses a multi-step pipeline with 18 specialized nodes
                - Code is safely executed in a sandboxed environment
                - The agent learns from previous queries to improve future responses
                
                ### Troubleshooting
                - **No response**: Make sure you have uploaded CSV files
                - **Wrong results**: Check that entity names match your data
                - **Slow response**: Complex queries may take 30-60 seconds
                """)
        
        gr.Markdown("""
        ---
        *Powered by PocketFlow and LLM*
        """, elem_classes=["footer-text"])
    
    return app

if __name__ == "__main__":
    app = create_app()
    app.launch(server_name="0.0.0.0", server_port=5000, share=False, css=CSS)
