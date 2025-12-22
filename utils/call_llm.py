from openai import OpenAI
import os

def call_llm(prompt):    
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    base_url = "https://openrouter.ai/api/v1"
    
    client = OpenAI(api_key=api_key, base_url=base_url)
    model = os.environ.get("LLM_MODEL", "meta-llama/llama-3.3-70b-instruct")
    
    r = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}]
    )
    return r.choices[0].message.content
    
if __name__ == "__main__":
    prompt = "What is the meaning of life?"
    print(call_llm(prompt))
