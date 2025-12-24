"""Tests for call_llm utility - LLM API wrapper with retry logic."""

import os
import time
from unittest.mock import MagicMock, Mock, patch

import pytest

from backend.utils.call_llm import call_llm


class TestCallLLMBasicFunctionality:
    """Test basic LLM calling functionality."""

    def test_successful_call(self, mock_openai_client, mock_env_vars):
        """Test a successful LLM API call."""
        mock_openai_client.chat.completions.create.return_value.choices[0].message.content = "Test response"

        result = call_llm("Test prompt")

        assert result == "Test response"
        mock_openai_client.chat.completions.create.assert_called_once()

    def test_uses_environment_api_key(self, mock_env_vars):
        """Test that API key is read from environment."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
            mock_client_class.return_value = mock_client

            call_llm("Test prompt")

            # Should be called with the API key from env
            mock_client_class.assert_called_once()
            call_args = mock_client_class.call_args
            assert call_args[1]["api_key"] == "test_api_key_12345"

    def test_uses_correct_base_url(self, mock_env_vars):
        """Test that OpenRouter base URL is used."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
            mock_client_class.return_value = mock_client

            call_llm("Test prompt")

            call_args = mock_client_class.call_args
            assert call_args[1]["base_url"] == "https://openrouter.ai/api/v1"

    def test_uses_timeout(self, mock_env_vars):
        """Test that timeout is set."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
            mock_client_class.return_value = mock_client

            call_llm("Test prompt")

            call_args = mock_client_class.call_args
            assert call_args[1]["timeout"] == 60.0


class TestCallLLMEnvironmentValidation:
    """Test environment variable validation."""

    def test_uses_default_api_key_when_missing(self):
        """Test that default API key is used when OPENROUTER_API_KEY is not set."""
        from backend.utils.call_llm import DEFAULT_API_KEY
        
        with patch.dict(os.environ, {}, clear=True):
            with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
                mock_client = MagicMock()
                mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
                mock_client_class.return_value = mock_client

                call_llm("Test prompt")

                # Should be called with the default API key
                mock_client_class.assert_called_once()
                call_args = mock_client_class.call_args
                assert call_args[1]["api_key"] == DEFAULT_API_KEY

    def test_uses_default_model_when_not_set(self, mock_env_vars):
        """Test default model is used when OPENROUTER_MODEL not set."""
        with patch.dict(os.environ, {"OPENROUTER_API_KEY": "test_key"}, clear=True):
            with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
                mock_client = MagicMock()
                mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
                mock_client_class.return_value = mock_client

                call_llm("Test prompt")

                # Should use default model
                call_args = mock_client.chat.completions.create.call_args
                assert call_args[1]["model"] == "meta-llama/llama-3.3-70b-instruct"

    def test_uses_custom_model_when_set(self, mock_env_vars):
        """Test custom model is used when OPENROUTER_MODEL is set."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
            mock_client_class.return_value = mock_client

            call_llm("Test prompt")

            call_args = mock_client.chat.completions.create.call_args
            assert call_args[1]["model"] == "test-model"


class TestCallLLMRetryLogic:
    """Test retry logic with exponential backoff."""

    def test_retries_on_failure(self, mock_env_vars):
        """Test that function retries on failure."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            # First two calls fail, third succeeds
            mock_client.chat.completions.create.side_effect = [
                Exception("Network error"),
                Exception("Network error"),
                MagicMock(choices=[MagicMock(message=MagicMock(content="Success"))])
            ]
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep") as mock_sleep:
                result = call_llm("Test prompt")

                assert result == "Success"
                # Should have been called 3 times
                assert mock_client.chat.completions.create.call_count == 3

    def test_exponential_backoff(self, mock_env_vars):
        """Test exponential backoff between retries."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            # First two calls fail, third succeeds
            mock_client.chat.completions.create.side_effect = [
                Exception("Error 1"),
                Exception("Error 2"),
                MagicMock(choices=[MagicMock(message=MagicMock(content="Success"))])
            ]
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep") as mock_sleep:
                call_llm("Test prompt")

                # Should sleep with exponential backoff: 2^1=2s, 2^2=4s
                assert mock_sleep.call_count == 2
                sleep_times = [call[0][0] for call in mock_sleep.call_args_list]
                assert sleep_times[0] == 2  # 2^(0+1)
                assert sleep_times[1] == 4  # 2^(1+1)

    def test_max_retries_three(self, mock_env_vars):
        """Test that max retries is 3."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            # Always fail
            mock_client.chat.completions.create.side_effect = Exception("Always fails")
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep"):
                with pytest.raises(RuntimeError, match="LLM call failed after 3 attempts"):
                    call_llm("Test prompt")

                # Should have been called exactly 3 times
                assert mock_client.chat.completions.create.call_count == 3

    def test_raises_runtime_error_after_max_retries(self, mock_env_vars):
        """Test that RuntimeError is raised after max retries."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            original_error = Exception("API Error")
            mock_client.chat.completions.create.side_effect = original_error
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep"):
                with pytest.raises(RuntimeError) as exc_info:
                    call_llm("Test prompt")

                # Should mention the number of attempts and original error
                assert "3 attempts" in str(exc_info.value)
                assert "API Error" in str(exc_info.value)

    def test_succeeds_on_first_try(self, mock_env_vars):
        """Test successful call on first attempt (no retries)."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Success"
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep") as mock_sleep:
                result = call_llm("Test prompt")

                assert result == "Success"
                # Should not sleep if successful on first try
                mock_sleep.assert_not_called()
                # Should only be called once
                assert mock_client.chat.completions.create.call_count == 1


class TestCallLLMMessageFormat:
    """Test message formatting."""

    def test_formats_message_correctly(self, mock_env_vars):
        """Test that message is formatted correctly for API."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
            mock_client_class.return_value = mock_client

            call_llm("Test prompt")

            call_args = mock_client.chat.completions.create.call_args
            messages = call_args[1]["messages"]

            assert len(messages) == 1
            assert messages[0]["role"] == "user"
            assert messages[0]["content"] == "Test prompt"


class TestCallLLMCustomMaxRetries:
    """Test custom max_retries parameter."""

    def test_respects_custom_max_retries(self, mock_env_vars):
        """Test that custom max_retries is respected."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.side_effect = Exception("Always fails")
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep"):
                with pytest.raises(RuntimeError, match="5 attempts"):
                    call_llm("Test prompt", max_retries=5)

                # Should have been called 5 times
                assert mock_client.chat.completions.create.call_count == 5

    def test_single_retry_attempt(self, mock_env_vars):
        """Test with max_retries=1 (no retries)."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.side_effect = Exception("Fails")
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep") as mock_sleep:
                with pytest.raises(RuntimeError, match="1 attempts"):
                    call_llm("Test prompt", max_retries=1)

                # Should only try once
                assert mock_client.chat.completions.create.call_count == 1
                # No sleep needed if only one attempt
                mock_sleep.assert_not_called()


class TestCallLLMErrorHandling:
    """Test various error scenarios."""

    def test_handles_network_error(self, mock_env_vars):
        """Test handling of network errors."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.side_effect = [
                Exception("Connection timeout"),
                MagicMock(choices=[MagicMock(message=MagicMock(content="Success"))])
            ]
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep"):
                result = call_llm("Test prompt")
                assert result == "Success"

    def test_handles_rate_limit_error(self, mock_env_vars):
        """Test handling of rate limit errors."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.side_effect = [
                Exception("Rate limit exceeded"),
                MagicMock(choices=[MagicMock(message=MagicMock(content="Success"))])
            ]
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep"):
                result = call_llm("Test prompt")
                assert result == "Success"

    def test_handles_authentication_error(self, mock_env_vars):
        """Test handling of authentication errors."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.side_effect = Exception("Invalid API key")
            mock_client_class.return_value = mock_client

            with patch("backend.utils.call_llm.time.sleep"):
                with pytest.raises(RuntimeError):
                    call_llm("Test prompt")


class TestCallLLMEdgeCases:
    """Test edge cases."""

    def test_handles_empty_prompt(self, mock_env_vars):
        """Test handling of empty prompt."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
            mock_client_class.return_value = mock_client

            result = call_llm("")

            assert result == "Response"

    def test_handles_very_long_prompt(self, mock_env_vars):
        """Test handling of very long prompts."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Response"
            mock_client_class.return_value = mock_client

            long_prompt = "A" * 100000
            result = call_llm(long_prompt)

            assert result == "Response"

    def test_handles_unicode_prompt(self, mock_env_vars):
        """Test handling of unicode characters in prompt."""
        with patch("backend.utils.call_llm.OpenAI") as mock_client_class:
            mock_client = MagicMock()
            mock_client.chat.completions.create.return_value.choices[0].message.content = "Réponse"
            mock_client_class.return_value = mock_client

            result = call_llm("Bonjour 世界 🌍")

            assert result == "Réponse"
