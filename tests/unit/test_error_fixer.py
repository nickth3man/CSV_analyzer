"""Tests for ErrorFixer node - retry logic and error handling."""

from backend.nodes import ErrorFixer


class TestErrorFixerRetryLogic:
    """Test retry counting and max retries enforcement."""

    def test_first_attempt_allows_retry(self):
        """Test that first attempt returns 'try_again'."""
        node = ErrorFixer()
        shared = {
            "exec_error": "NameError: name 'x' is not defined",
            "csv_code_snippet": "final_result = x",
            "retry_count": 0,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert exec_res == "try_again"

    def test_second_attempt_allows_retry(self):
        """Test that second attempt returns 'try_again'."""
        node = ErrorFixer()
        shared = {
            "exec_error": "NameError: name 'x' is not defined",
            "csv_code_snippet": "final_result = x",
            "retry_count": 1,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert exec_res == "try_again"

    def test_third_attempt_allows_retry(self):
        """Test that third attempt (last one) returns 'try_again'."""
        node = ErrorFixer()
        shared = {
            "exec_error": "NameError: name 'x' is not defined",
            "csv_code_snippet": "final_result = x",
            "retry_count": 2,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert exec_res == "try_again"

    def test_max_retries_exceeded(self):
        """Test that after max retries, returns 'max_retries_exceeded'."""
        node = ErrorFixer()
        shared = {
            "exec_error": "NameError: name 'x' is not defined",
            "csv_code_snippet": "final_result = x",
            "retry_count": 3,  # Already tried 3 times
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert exec_res == "max_retries_exceeded"

    def test_way_over_max_retries(self):
        """Test that even if retry_count is way over, it's still exceeded."""
        node = ErrorFixer()
        shared = {
            "exec_error": "Some error",
            "csv_code_snippet": "some code",
            "retry_count": 10,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)

        assert exec_res == "max_retries_exceeded"


class TestErrorFixerPostMethod:
    """Test the post() method behavior."""

    def test_post_try_again(self):
        """Test post() when retry is allowed."""
        node = ErrorFixer()
        shared = {
            "exec_error": "Error message",
            "csv_code_snippet": "code",
            "retry_count": 0,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "fix"
        assert shared["retry_count"] == 1

    def test_post_increments_retry_count(self):
        """Test that post() increments retry_count."""
        node = ErrorFixer()
        shared = {
            "exec_error": "Error message",
            "csv_code_snippet": "code",
            "retry_count": 1,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "fix"
        assert shared["retry_count"] == 2

    def test_post_give_up(self):
        """Test post() when max retries exceeded."""
        node = ErrorFixer()
        shared = {
            "exec_error": "Persistent error",
            "csv_code_snippet": "bad code",
            "retry_count": 3,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "give_up"
        assert "final_text" in shared
        assert "Unable to answer" in shared["final_text"]
        assert "Persistent error" in shared["final_text"]

    def test_post_initializes_retry_count_if_missing(self):
        """Test that post() initializes retry_count if not present."""
        node = ErrorFixer()
        shared = {
            "exec_error": "Error message",
            "code_snippet": "code",
            # No retry_count key
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "fix"
        assert shared["retry_count"] == 1


class TestErrorFixerPrepMethod:
    """Test the prep() method."""

    def test_prep_with_retry_count(self):
        """Test prep() when retry_count exists."""
        node = ErrorFixer()
        shared = {
            "exec_error": "Some error",
            "csv_code_snippet": "some code",
            "retry_count": 2,
        }

        error, code, retry_count = node.prep(shared)

        assert error == "Some error"
        assert code["csv"] == "some code"
        assert retry_count == 2

    def test_prep_without_retry_count(self):
        """Test prep() when retry_count doesn't exist."""
        node = ErrorFixer()
        shared = {"exec_error": "Some error", "csv_code_snippet": "some code"}

        error, code, retry_count = node.prep(shared)

        assert error == "Some error"
        assert code["csv"] == "some code"
        assert retry_count == 0  # Defaults to 0


class TestErrorFixerMaxRetriesConstant:
    """Test the MAX_RETRIES constant."""

    def test_max_retries_is_three(self):
        """Test that MAX_RETRIES is set to 3."""
        node = ErrorFixer()
        assert node.MAX_RETRIES == 3

    def test_boundary_at_max_retries(self):
        """Test behavior exactly at MAX_RETRIES boundary."""
        node = ErrorFixer()

        # retry_count = MAX_RETRIES - 1 should allow retry
        shared_just_under = {
            "exec_error": "Error",
            "csv_code_snippet": "code",
            "retry_count": node.MAX_RETRIES - 1,
        }
        prep_res = node.prep(shared_just_under)
        exec_res = node.exec(prep_res)
        assert exec_res == "try_again"

        # retry_count = MAX_RETRIES should give up
        shared_at_max = {
            "exec_error": "Error",
            "csv_code_snippet": "code",
            "retry_count": node.MAX_RETRIES,
        }
        prep_res = node.prep(shared_at_max)
        exec_res = node.exec(prep_res)
        assert exec_res == "max_retries_exceeded"


class TestErrorFixerErrorMessages:
    """Test error message handling."""

    def test_preserves_error_message_in_final_text(self):
        """Test that the error message is included in final_text when giving up."""
        node = ErrorFixer()
        error_msg = "KeyError: 'nonexistent_column'"
        shared = {
            "exec_error": error_msg,
            "csv_code_snippet": "df['nonexistent_column']",
            "retry_count": 3,
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "give_up"
        assert error_msg in shared["final_text"]

    def test_handles_missing_exec_error(self):
        """Test behavior when exec_error is missing."""
        node = ErrorFixer()
        shared = {"csv_code_snippet": "some code", "retry_count": 3}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "give_up"
        assert "Unknown" in shared["final_text"]


class TestErrorFixerFullCycle:
    """Test full retry cycle."""

    def test_full_retry_cycle(self):
        """Test a complete cycle from first error to give up."""
        node = ErrorFixer()
        shared = {"exec_error": "Initial error", "csv_code_snippet": "bad code"}

        # First attempt (retry_count starts at 0)
        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)
        assert action == "fix"
        assert shared["retry_count"] == 1

        # Second attempt
        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)
        assert action == "fix"
        assert shared["retry_count"] == 2

        # Third attempt
        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)
        assert action == "fix"
        assert shared["retry_count"] == 3

        # Fourth attempt - should give up
        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)
        assert action == "give_up"
        assert "final_text" in shared
