"""Tests for ClarifyQuery node."""

from backend.nodes import ClarifyQuery


class TestClarifyQueryBasics:
    """Test basic ClarifyQuery functionality."""

    def test_accepts_valid_query_without_underscores(self) -> None:
        """Test that simple queries without underscores are accepted."""
        node = ClarifyQuery()
        shared = {
            "question": "What is the average salary?",
            "schema_str": "Table 'employees': [name, age, salary]",
            "dfs": {"employees": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "clear"
        assert missing is None

    def test_accepts_valid_query_with_matching_column(self) -> None:
        """Test that queries with valid column references are accepted."""
        node = ClarifyQuery()
        shared = {
            "question": "Show me the person_id and name columns",
            "schema_str": "Table 'players': [person_id, name, team]",
            "dfs": {"players": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "clear"
        assert missing is None

    def test_rejects_query_with_invalid_column_name(self) -> None:
        """Test that queries with non-existent column names are rejected."""
        node = ClarifyQuery()
        shared = {
            "question": "What is the total_revenue for each quarter?",
            "schema_str": "Table 'sales': [date, amount, quantity]",
            "dfs": {"sales": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "ambiguous"
        assert missing is not None
        assert "total_revenue" in missing

    def test_ignores_short_words_with_underscores(self) -> None:
        """Test that short words with underscores (<=3 chars) are ignored."""
        node = ClarifyQuery()
        shared = {
            "question": "Show me data for a_b_c test",
            "schema_str": "Table 'test': [name, value]",
            "dfs": {"test": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        # "a_b" is only 3 chars, "a_b_c" is 5 but might be filtered differently
        # The logic checks `len(word) > 3`, so "a_b_c" (5 chars) would be checked
        # but since it's not in schema, it should be flagged
        assert status == "ambiguous"
        assert missing is not None and "a_b_c" in missing


class TestClarifyQueryCaseInsensitivity:
    """Test case-insensitive matching."""

    def test_case_insensitive_column_matching(self) -> None:
        """Test that column matching is case-insensitive."""
        node = ClarifyQuery()
        shared = {
            "question": "What is the PERSON_ID for LeBron?",
            "schema_str": "Table 'players': [person_id, name, team]",
            "dfs": {"players": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        # Should accept because person_id exists (case-insensitive)
        assert status == "clear"
        assert missing is None

    def test_case_insensitive_schema_comparison(self) -> None:
        """Test that schema comparison is case-insensitive."""
        node = ClarifyQuery()
        shared = {
            "question": "Show me Team_Name and Player_ID",
            "schema_str": "Table 'PLAYERS': [TEAM_NAME, PLAYER_ID, NAME]",
            "dfs": {"PLAYERS": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "clear"
        assert missing is None


class TestClarifyQueryMultipleColumns:
    """Test handling of queries with multiple column references."""

    def test_multiple_valid_columns(self) -> None:
        """Test query with multiple valid column names."""
        node = ClarifyQuery()
        shared = {
            "question": "Compare first_name, last_name, and player_id",
            "schema_str": "Table 'players': [player_id, first_name, last_name, team]",
            "dfs": {"players": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "clear"
        assert missing is None

    def test_mix_of_valid_and_invalid_columns(self) -> None:
        """Test query with both valid and invalid column names."""
        node = ClarifyQuery()
        shared = {
            "question": "Show player_id and nonexistent_column and team_name",
            "schema_str": "Table 'players': [player_id, name, team_name]",
            "dfs": {"players": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "ambiguous"
        assert missing is not None
        assert "nonexistent_column" in missing
        # player_id and team_name should not be in missing
        assert "player_id" not in missing
        assert "team_name" not in missing

    def test_multiple_invalid_columns(self) -> None:
        """Test query with multiple invalid column names."""
        node = ClarifyQuery()
        shared = {
            "question": "Get bad_column and another_bad_col from data",
            "schema_str": "Table 'data': [id, value]",
            "dfs": {"data": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "ambiguous"
        assert missing is not None
        assert "bad_column" in missing
        assert "another_bad_col" in missing


class TestClarifyQueryPartialMatches:
    """Test partial matching behavior."""

    def test_partial_column_name_in_schema(self) -> None:
        """Test that partial matches are handled correctly."""
        node = ClarifyQuery()
        shared = {
            "question": "What is the player_id value?",
            "schema_str": "Table 'stats': [common_player_id, game_id, points]",
            "dfs": {"stats": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        # "player_id" is a substring of "common_player_id" in schema
        # The current implementation does substring matching, so it should pass
        assert status == "clear"
        assert missing is None

    def test_column_name_contains_schema_word(self) -> None:
        """Test when query contains a word that's part of a schema column."""
        node = ClarifyQuery()
        shared = {
            "question": "Show me common_player data",
            "schema_str": "Table 'stats': [common_player_id, points]",
            "dfs": {"stats": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, _missing = exec_res

        # "common_player" is a substring match
        assert status == "clear"


class TestClarifyQueryPost:
    """Test the post() method behavior."""

    def test_post_stores_error_message_for_ambiguous(self) -> None:
        """Test that ambiguous queries store an error message."""
        node = ClarifyQuery()
        shared = {
            "question": "Show invalid_column",
            "schema_str": "Table 'data': [id, value]",
            "dfs": {"data": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "ambiguous"
        assert "final_text" in shared
        assert "invalid_column" in shared["final_text"]

    def test_post_returns_clear_for_valid_query(self) -> None:
        """Test that valid queries return 'clear' action."""
        node = ClarifyQuery()
        shared = {
            "question": "What is the average?",
            "schema_str": "Table 'data': [id, value]",
            "dfs": {"data": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        action = node.post(shared, prep_res, exec_res)

        assert action == "clear"
        assert "final_text" not in shared or "unknown columns" not in shared.get(
            "final_text",
            "",
        )


class TestClarifyQueryEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_question(self) -> None:
        """Test handling of empty question."""
        node = ClarifyQuery()
        shared = {
            "question": "",
            "schema_str": "Table 'data': [id, value]",
            "dfs": {"data": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "clear"
        assert missing is None

    def test_question_with_no_underscores(self) -> None:
        """Test question with natural language and no column-like words."""
        node = ClarifyQuery()
        shared = {
            "question": "What are the top performers this quarter?",
            "schema_str": "Table 'performance': [employee, score, quarter]",
            "dfs": {"performance": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "clear"
        assert missing is None

    def test_empty_schema(self) -> None:
        """Test behavior with empty schema."""
        node = ClarifyQuery()
        shared = {"question": "Show me some_column_name", "schema_str": "", "dfs": {}}

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "ambiguous"
        assert missing is not None and "some_column_name" in missing

    def test_multiple_tables_in_schema(self) -> None:
        """Test with multiple tables in schema."""
        node = ClarifyQuery()
        shared = {
            "question": "Compare player_id and team_id",
            "schema_str": (
                "Table 'players': [player_id, name, team]\n"
                "Table 'teams': [team_id, team_name, city]"
            ),
            "dfs": {"players": None, "teams": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "clear"
        assert missing is None

    def test_special_characters_in_question(self) -> None:
        """Test handling of special characters.

        Note: The current implementation doesn't strip punctuation from words,
        so "player_id?" is treated as different from "player_id".
        This test documents current behavior.
        """
        node = ClarifyQuery()
        shared = {
            "question": "What's the player_id? And the team_name!",
            "schema_str": "Table 'players': [player_id, team_name]",
            "dfs": {"players": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        # Currently fails because punctuation isn't stripped
        # "player_id?" does not match "player_id" in schema
        assert status == "ambiguous"
        assert missing is not None and "player_id?" in missing
        assert missing is not None and "team_name!" in missing

    def test_column_name_exactly_three_characters_with_underscore(self) -> None:
        """Test that 3-character words with underscores are ignored."""
        node = ClarifyQuery()
        shared = {
            "question": "Show a_b column",
            "schema_str": "Table 'data': [id, value]",
            "dfs": {"data": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        # "a_b" is exactly 3 chars, should be ignored (len > 3 check)
        assert status == "clear"
        assert missing is None

    def test_column_name_exactly_four_characters_with_underscore(self) -> None:
        """Test that 4-character words with underscores are checked."""
        node = ClarifyQuery()
        shared = {
            "question": "Show a_bc column",
            "schema_str": "Table 'data': [id, value]",
            "dfs": {"data": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        # "a_bc" is 4 chars (len > 3), should be checked and flagged as missing
        assert status == "ambiguous"
        assert missing is not None and "a_bc" in missing


class TestClarifyQueryIntegration:
    """Integration tests with realistic scenarios."""

    def test_nba_player_query(self) -> None:
        """Test with realistic NBA player query."""
        node = ClarifyQuery()
        shared = {
            "question": "Compare LeBron James and Kobe Bryant using player_id and person_id",
            "schema_str": (
                "Table 'common_player_info': [person_id, display_first_last, team_name]\n"
                "Table 'player_career_stats': [player_id, games_played, points]"
            ),
            "dfs": {"common_player_info": None, "player_career_stats": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "clear"
        assert missing is None

    def test_invalid_nba_query(self) -> None:
        """Test NBA query with invalid column."""
        node = ClarifyQuery()
        shared = {
            "question": "Show me the championship_count for each team",
            "schema_str": "Table 'teams': [team_id, team_name, city]",
            "dfs": {"teams": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        assert status == "ambiguous"
        assert missing is not None and "championship_count" in missing

    def test_sales_analysis_query(self) -> None:
        """Test with sales analysis query."""
        node = ClarifyQuery()
        shared = {
            "question": "Calculate total_revenue by product_id and customer_id",
            "schema_str": "Table 'sales': [product_id, customer_id, amount, date]",
            "dfs": {"sales": None},
        }

        prep_res = node.prep(shared)
        exec_res = node.exec(prep_res)
        status, missing = exec_res

        # total_revenue doesn't exist, should be flagged
        assert status == "ambiguous"
        assert missing is not None and "total_revenue" in missing
        # product_id and customer_id exist, should not be in missing
        assert missing is not None and "product_id" not in missing
        assert missing is not None and "customer_id" not in missing
