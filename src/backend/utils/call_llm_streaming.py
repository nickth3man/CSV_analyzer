import os

from openai import OpenAI

from backend.utils.call_llm import (
    DEFAULT_MAX_TOKENS,
    DEFAULT_MODEL,
    DEFAULT_TEMPERATURE,
)


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
        raise RuntimeError(
            "OPENROUTER_API_KEY is required. Set it in the environment or .env.",
        )

    base_url = "https://openrouter.ai/api/v1"

    client = OpenAI(api_key=api_key, base_url=base_url)
    if model is None:
        model = (
            os.environ.get("OPENROUTER_MODEL")
            or os.environ.get("LLM_MODEL")
            or DEFAULT_MODEL
        )

    temperature = DEFAULT_TEMPERATURE
    if env_temp := os.environ.get("LLM_TEMPERATURE"):
        try:
            temperature = float(env_temp)
        except ValueError:
            pass

    max_tokens = DEFAULT_MAX_TOKENS
    if env_tokens := os.environ.get("LLM_MAX_TOKENS"):
        try:
            max_tokens = int(env_tokens)
        except ValueError:
            pass

    kwargs = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": True,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

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
