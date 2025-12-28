import os

from openai import OpenAI

from backend.utils.call_llm import DEFAULT_API_KEY, should_force_chutes_provider


def call_llm_streaming(prompt, model=None):
    """Stream response tokens from an OpenRouter-backed language model for the given prompt.

    Parameters:
        prompt (str): Text prompt sent as the user's message to the model.
        model (str, optional): Model identifier to use. If omitted, the function uses the OPENROUTER_MODEL environment variable or the default "meta-llama/llama-3.3-70b-instruct".

    Returns:
        generator: An iterator that yields successive string tokens (pieces of response text) as they become available from the streaming API.
    """
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        api_key = DEFAULT_API_KEY
        os.environ["OPENROUTER_API_KEY"] = api_key

    base_url = "https://openrouter.ai/api/v1"

    client = OpenAI(api_key=api_key, base_url=base_url)
    if model is None:
        model = os.environ.get("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct")

    # Build extra_body for provider routing if needed
    extra_body = None
    if should_force_chutes_provider(model, api_key):
        extra_body = {"provider": {"only": ["chutes"], "allow_fallbacks": False}}

    kwargs = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
    }
    if extra_body:
        kwargs["extra_body"] = extra_body

    stream = client.chat.completions.create(**kwargs)

    for chunk in stream:
        if chunk.choices[0].delta.content is not None:
            yield chunk.choices[0].delta.content


def call_llm_with_callback(prompt, callback=None, model=None):
    full_response = ""
    for token in call_llm_streaming(prompt, model):
        full_response += token
        if callback:
            callback(token)
    return full_response
