"""Configuration and constants for the Chainlit frontend."""

import logging
import os

import requests


logger = logging.getLogger(__name__)

# Default API key for testing (limited access, short expiration)
DEFAULT_API_KEY = "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"

# Models hosted by Chutes provider (base model IDs without variant suffixes)
# These models have Chutes as one of their available providers
CHUTES_HOSTED_MODELS = {
    "arliai/qwq-32b-arliai-rpr-v1",
    "deepseek/deepseek-chat",
    "deepseek/deepseek-chat-v3-0324",
    "deepseek/deepseek-chat-v3.1",
    "deepseek/deepseek-r1",
    "deepseek/deepseek-r1-0528",
    "deepseek/deepseek-r1-0528-qwen3-8b",
    "deepseek/deepseek-r1-distill-llama-70b",
    "deepseek/deepseek-v3.1-terminus",
    "deepseek/deepseek-v3.2",
    "deepseek/deepseek-v3.2-speciale",
    "google/gemma-3-12b-it",
    "google/gemma-3-27b-it",
    "google/gemma-3-4b-it",
    "minimax/minimax-m2",
    "mistralai/devstral-2512",
    "mistralai/mistral-nemo",
    "mistralai/mistral-small-24b-instruct-2501",
    "mistralai/mistral-small-3.1-24b-instruct",
    "mistralai/mistral-small-3.2-24b-instruct",
    "moonshotai/kimi-k2-0905",
    "moonshotai/kimi-k2-thinking",
    "nousresearch/deephermes-3-mistral-24b-preview",
    "nousresearch/hermes-4-405b",
    "nousresearch/hermes-4-70b",
    "nvidia/nemotron-3-nano-30b-a3b",
    "openai/gpt-oss-120b",
    "opengvlab/internvl3-78b",
    "qwen/qwen-2.5-72b-instruct",
    "qwen/qwen-2.5-coder-32b-instruct",
    "qwen/qwen2.5-vl-32b-instruct",
    "qwen/qwen2.5-vl-72b-instruct",
    "qwen/qwen3-14b",
    "qwen/qwen3-235b-a22b",
    "qwen/qwen3-235b-a22b-2507",
    "qwen/qwen3-235b-a22b-thinking-2507",
    "qwen/qwen3-30b-a3b",
    "qwen/qwen3-30b-a3b-instruct-2507",
    "qwen/qwen3-32b",
    "qwen/qwen3-coder",
    "qwen/qwen3-next-80b-a3b-instruct",
    "qwen/qwen3-vl-235b-a22b-instruct",
    "qwen/qwen3-vl-235b-a22b-thinking",
    "tngtech/deepseek-r1t-chimera",
    "tngtech/deepseek-r1t2-chimera",
    "tngtech/tng-r1t-chimera",
    "z-ai/glm-4.5",
    "z-ai/glm-4.6",
    "z-ai/glm-4.6v",
    "z-ai/glm-4.7",
}

DEFAULT_MODELS = [
    "deepseek/deepseek-r1-0528:free",
    "mistralai/mistral-small-3.1-24b-instruct:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "mistralai/mistral-7b-instruct:free"
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


def is_free_model(model):
    """Check if a model is free (both prompt and completion pricing are 0)."""
    pricing = model.get("pricing", {})
    return pricing.get("prompt") == "0" and pricing.get("completion") == "0"


def is_chutes_model(model_id):
    """
    Check if a model is hosted by the Chutes provider.
    Handles both base model IDs and variant suffixes (e.g., :free, :nitro).
    """
    # Get base model ID (without variant suffix like :free, :nitro)
    base_id = model_id.split(":")[0] if ":" in model_id else model_id
    return base_id in CHUTES_HOSTED_MODELS


def is_allowed_model(model) -> bool:
    """
    Check if a model should be included in the filtered list.
    Returns True if:
    - Model is free (pricing.prompt == 0 and pricing.completion == 0)
    - Model is from MistralAI (id starts with 'mistralai/')
    - Model is hosted by Chutes provider.
    """
    model_id = model.get("id", "")

    # Check if it's a free model
    if is_free_model(model):
        return True

    # Check if it's a MistralAI model
    if model_id.startswith("mistralai/"):
        return True

    # Check if it's hosted by Chutes provider
    return bool(is_chutes_model(model_id))


def fetch_openrouter_models(api_key=None, filter_models=True):
    """
    Fetch available models from OpenRouter API.

    Args:
        api_key: OpenRouter API key. If None, uses environment variable or default.
        filter_models: If True, filter to only show free models and MistralAI models.
                      This is used when the default API key is in use.

    Returns:
        List of model IDs.
    """
    if not api_key:
        api_key = os.environ.get("OPENROUTER_API_KEY", "")

    # Use default API key if none provided
    if not api_key:
        api_key = DEFAULT_API_KEY
        filter_models = True  # Always filter when using default key

    # Check if we're using the default API key
    using_default_key = (api_key == DEFAULT_API_KEY)
    if using_default_key:
        filter_models = True

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
                if not model_id:
                    continue

                # Apply filtering if using default key or filter requested
                if filter_models:
                    if is_allowed_model(model):
                        models.append(model_id)
                else:
                    models.append(model_id)

            models.sort()
            return models if models else DEFAULT_MODELS
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"Warning: Could not fetch OpenRouter models: {e}")

    return DEFAULT_MODELS
