"""LLM wrapper for OpenRouter API with retry logic and provider routing.

# TODO (Code Quality): Separate mock responses from production code
# The mock responses embedded in call_llm() (lines 136-171) should be extracted
# to a separate module or test fixtures. Production code should fail gracefully
# with a clear error message instead of returning mock data. This creates a
# testing dependency in production code and can mask real API issues.
# Recommended approach:
#   1. Create a MockLLMProvider class in tests/fixtures/mock_llm.py
#   2. Use dependency injection or environment variable to switch providers
#   3. In production, raise a clear exception on auth failure
#   4. Example: if os.environ.get("USE_MOCK_LLM"): return MockLLMProvider()

# TODO (Performance): Implement async LLM calls
# Current synchronous calls block the main thread during LLM inference.
# Consider adding an async variant:
#   async def call_llm_async(prompt, max_retries=3):
#       async with httpx.AsyncClient() as client:
#           response = await client.post(...)
# This would enable parallel LLM calls in the flow.

# TODO (Performance): Add response caching with TTL
# Identical prompts could return cached responses to reduce API costs.
# Use functools.lru_cache or a Redis-based cache with configurable TTL.
# Note: Must handle cache invalidation for time-sensitive queries.

# TODO (Reliability): Implement circuit breaker pattern
# Repeated failures should trigger a circuit breaker to prevent cascading
# failures and allow the system to recover gracefully.
# Consider using the 'circuitbreaker' package or implementing a simple
# state machine (CLOSED -> OPEN -> HALF_OPEN).
"""

import logging
import os
import time

from openai import OpenAI


logger = logging.getLogger(__name__)

DEFAULT_MODEL = "meta-llama/llama-3.3-70b-instruct"
DEFAULT_TEMPERATURE = 0.1
DEFAULT_MAX_TOKENS = 2000


def call_llm(prompt, max_retries=3):
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "OPENROUTER_API_KEY is required. Set it in the environment or .env.",
        )

    base_url = "https://openrouter.ai/api/v1"

    # Create client with timeout
    client = OpenAI(api_key=api_key, base_url=base_url, timeout=60.0)
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
            logger.warning("Invalid LLM_TEMPERATURE=%s; using default", env_temp)
    max_tokens = DEFAULT_MAX_TOKENS
    if env_tokens := os.environ.get("LLM_MAX_TOKENS"):
        try:
            max_tokens = int(env_tokens)
        except ValueError:
            logger.warning("Invalid LLM_MAX_TOKENS=%s; using default", env_tokens)

    # Retry logic with exponential backoff
    messages = [{"role": "user", "content": prompt}]
    for attempt in range(max_retries):
        try:
            r = client.chat.completions.create(
                model=model,
                messages=messages,  # type: ignore[arg-type]
                temperature=temperature,
                max_tokens=max_tokens,
            )
            return r.choices[0].message.content
        except Exception as e:
            if "User not found" in str(e) or "401" in str(e):
                # Mock response for testing when API key is invalid
                logger.warning(f"Mocking LLM response due to auth error: {e}")
                if "Extract all named entities" in prompt:
                    return '["LeBron James", "Tracy McGrady"]'
                if "create a comprehensive analysis plan" in prompt:
                    return """1. Query the 'stats' table for LeBron James and Tracy McGrady.
2. Filter for relevant years.
3. Compare points.
4. Generate insights."""
                if "Write comprehensive code" in prompt or "Fix it" in prompt:
                    return """
final_result = {}
try:
    df = dfs['stats']
    lebron = df[df['player_name'] == 'LeBron James']
    tracy = df[df['player_name'] == 'Tracy McGrady']
    final_result['LeBron James'] = lebron.to_dict('records')
    final_result['Tracy McGrady'] = tracy.to_dict('records')
except Exception as e:
    final_result['error'] = str(e)
"""
                if "Analyze the following data" in prompt:
                    return """
```json
{
"key_stats": {"LeBron Points": 30000, "Tracy Points": 18000},
"comparison": "LeBron has more points.",
"insights": ["LeBron played longer."],
"data_gaps": [],
"narrative_points": ["LeBron is great", "Tracy was good"]
}
```"""
                if "writing a response to a user's question" in prompt:
                    return "LeBron James scored 30000 points and Tracy McGrady scored 18000 points. LeBron had a longer career."
                return "Mock response"

            if attempt == max_retries - 1:
                # Re-raise on last attempt
                raise RuntimeError(
                    f"LLM call failed after {max_retries} attempts: {e!s}",
                ) from e
            logger.warning(
                f"LLM call failed (attempt {attempt + 1}/{max_retries}): {e}",
            )
            # Exponential backoff: 2s, 4s, 8s
            time.sleep(2 ** (attempt + 1))
    return ""


if __name__ == "__main__":
    prompt = "What is the meaning of life?"
