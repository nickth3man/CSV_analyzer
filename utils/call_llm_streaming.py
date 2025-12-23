from openai import OpenAI
import os

def call_llm_streaming(prompt, model=None):
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    base_url = "https://openrouter.ai/api/v1"
    
    client = OpenAI(api_key=api_key, base_url=base_url)
    if model is None:
        model = os.environ.get("LLM_MODEL", "meta-llama/llama-3.3-70b-instruct")
    
    stream = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        stream=True
    )
    
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
