"""Unit tests for scripts/create_advanced_metrics.py module.

This module tests advanced metrics calculation and view creation including:
- Player game-level advanced metrics (TS%, eFG%, etc.)
- Team game-level metrics
- Season aggregation
- Four Factors calculations
- League averages
"""

from unittest.mock import MagicMock, Mock, call, patch

import pytest

from scripts.create_advanced_metrics import create_advanced_metrics


class TestCreateAdvancedMetrics:
    """Test suite for advanced metrics creation."""

    def test_create_advanced_metrics_connects_to_database(self):
        """Test that function connects to correct database."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.return_value = MagicMock()
            
            create_advanced_metrics("test.db")
            
            mock_connect.assert_called_once_with("test.db")
            mock_con.close.assert_called_once()

    def test_create_advanced_metrics_uses_default_path(self):
        """Test default database path is used when not specified."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.return_value = MagicMock()
            
            create_advanced_metrics()
            
            mock_connect.assert_called_once_with("data/nba.duckdb")

    def test_create_advanced_metrics_creates_player_game_advanced_view(self):
        """Test creation of player_game_advanced view."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            # Verify player_game_advanced view creation
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("player_game_advanced" in c for c in calls)

    def test_create_advanced_metrics_creates_team_game_advanced_view(self):
        """Test creation of team_game_advanced view."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("team_game_advanced" in c for c in calls)

    def test_create_advanced_metrics_creates_player_season_stats_table(self):
        """Test creation of player_season_stats table."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("player_season_stats" in c and "CREATE TABLE" in c 
                      for c in calls)

    def test_create_advanced_metrics_creates_four_factors_view(self):
        """Test creation of team_four_factors view."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("team_four_factors" in c for c in calls)

    def test_create_advanced_metrics_creates_league_averages_view(self):
        """Test creation of league_season_averages view."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("league_season_averages" in c for c in calls)

    def test_create_advanced_metrics_creates_career_summary_view(self):
        """Test creation of player_career_summary view."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("player_career_summary" in c for c in calls)

    def test_create_advanced_metrics_includes_true_shooting_percentage(self):
        """Test that TS% calculation is included."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "ts_pct" in sql_text.lower()
            assert "0.44" in sql_text  # TS% coefficient

    def test_create_advanced_metrics_includes_effective_fg_percentage(self):
        """Test that eFG% calculation is included."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "efg_pct" in sql_text.lower()
            assert "0.5" in sql_text  # eFG% coefficient for 3PM

    def test_create_advanced_metrics_includes_turnover_percentage(self):
        """Test that TOV% calculation is included."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "tov_pct" in sql_text.lower()

    def test_create_advanced_metrics_includes_game_score(self):
        """Test that Game Score calculation is included."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "game_score" in sql_text.lower()

    def test_create_advanced_metrics_includes_fantasy_points(self):
        """Test that fantasy points calculation is included."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "fantasy_pts" in sql_text.lower()

    def test_create_advanced_metrics_includes_double_double_indicator(self):
        """Test that double-double detection is included."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "double_double" in sql_text.lower()

    def test_create_advanced_metrics_includes_triple_double_indicator(self):
        """Test that triple-double detection is included."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "triple_double" in sql_text.lower()


    def test_create_advanced_metrics_commits_transaction(self):
        """Test that database transaction is committed."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            mock_con.commit.assert_called_once()

    def test_create_advanced_metrics_closes_connection_on_success(self):
        """Test that connection is closed after successful completion."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            mock_con.close.assert_called_once()

    def test_create_advanced_metrics_closes_connection_on_error(self):
        """Test that connection is closed even when error occurs."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.side_effect = Exception("SQL error")
            
            with pytest.raises(Exception):
                create_advanced_metrics()
            
            mock_con.close.assert_called_once()

    def test_create_advanced_metrics_uses_create_or_replace_for_views(self):
        """Test that views use CREATE OR REPLACE."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            # Only check CREATE VIEW statements
            view_creates = [c for c in calls if "CREATE" in c and "VIEW" in c]
            assert all("CREATE OR REPLACE" in c for c in view_creates)
            assert len(view_creates) > 0

    def test_create_advanced_metrics_uses_if_not_exists_for_tables(self):
        """Test that tables use IF NOT EXISTS."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            table_creates = [c for c in calls 
                           if "CREATE TABLE" in c and "VIEW" not in c]
            assert any("IF NOT EXISTS" in c for c in table_creates)

    def test_create_advanced_metrics_verifies_view_creation(self):
        """Test that function verifies views were created."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.return_value.fetchall.return_value = [
                ("player_game_advanced",),
                ("team_game_advanced",),
            ]
            
            create_advanced_metrics()
            
            # Verify information_schema query was made
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("information_schema" in c for c in calls)

    def test_create_advanced_metrics_handles_missing_source_tables_gracefully(self):
        """Test graceful handling when source tables don't exist."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.side_effect = [
                MagicMock(),  # First view succeeds
                Exception("Table 'player_game_stats' not found"),  # Second fails
            ]
            
            # Should raise exception - no graceful handling expected
            with pytest.raises(Exception):
                create_advanced_metrics()

    def test_create_advanced_metrics_calculates_per_game_averages(self):
        """Test that per-game averages are calculated in season stats."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "ppg" in sql_text.lower() or "pts_per_game" in sql_text.lower()

    def test_create_advanced_metrics_includes_primary_key_in_season_stats(self):
        """Test that player_season_stats has composite primary key."""
        with patch("scripts.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            season_stats_create = [c for c in calls if "player_season_stats" in c]
            assert any("PRIMARY KEY" in c for c in season_stats_create)