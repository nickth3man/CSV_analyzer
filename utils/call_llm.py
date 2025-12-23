from openai import OpenAI
import os
import time

def call_llm(prompt, max_retries=3):
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY environment variable not set")

    base_url = "https://openrouter.ai/api/v1"

    # Create client with timeout
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=60.0)
    model = os.environ.get("LLM_MODEL", "meta-llama/llama-3.3-70b-instruct")

    # Retry logic with exponential backoff
    for attempt in range(max_retries):
        try:
            r = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}]
            )
            return r.choices[0].message.content
        except Exception as e:
            if attempt == max_retries - 1:
                # Re-raise on last attempt
                raise RuntimeError(f"LLM call failed after {max_retries} attempts: {str(e)}") from e
            print(f"LLM call failed (attempt {attempt+1}/{max_retries}): {e}")
            # Exponential backoff: 2s, 4s, 8s
            time.sleep(2 ** (attempt + 1))
    
if __name__ == "__main__":
    prompt = "What is the meaning of life?"
    print(call_llm(prompt))
