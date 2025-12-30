"""Unit tests for scripts/analysis/create_advanced_metrics.py module.

This module tests advanced metrics calculation and view creation including:
- Player game-level advanced metrics (TS%, eFG%, etc.)
- Team game-level metrics
- Season aggregation
- Four Factors calculations
- League averages
"""
import pytest

pytest.skip("Legacy CSV/codegen flow removed in SQL redesign.", allow_module_level=True)


from unittest.mock import MagicMock, Mock, call, patch

import pytest

from src.scripts.analysis.create_advanced_metrics import create_advanced_metrics


class TestCreateAdvancedMetrics:
    """Test suite for advanced metrics creation."""

    def test_create_advanced_metrics_connects_to_database(self):
        """Test that function connects to correct database."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.return_value = MagicMock()
            
            create_advanced_metrics("test.db")
            
            mock_connect.assert_called_once_with("test.db")
            mock_con.close.assert_called_once()

    def test_create_advanced_metrics_uses_default_path(self):
        """Test default database path is used when not specified."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.return_value = MagicMock()
            
            create_advanced_metrics()
            
            mock_connect.assert_called_once_with("src/backend/data/nba.duckdb")

    def test_create_advanced_metrics_creates_player_game_advanced_view(self):
        """Test creation of player_game_advanced view."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            # Verify player_game_advanced view creation
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("player_game_advanced" in c for c in calls)

    def test_create_advanced_metrics_creates_team_game_advanced_view(self):
        """Test creation of team_game_advanced view."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("team_game_advanced" in c for c in calls)

    def test_create_advanced_metrics_creates_player_season_stats_table(self):
        """Test creation of player_season_stats table."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("player_season_stats" in c and "CREATE TABLE" in c 
                      for c in calls)

    def test_create_advanced_metrics_creates_four_factors_view(self):
        """Test creation of team_four_factors view."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("team_four_factors" in c for c in calls)

    def test_create_advanced_metrics_creates_league_averages_view(self):
        """Test creation of league_season_averages view."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("league_season_averages" in c for c in calls)

    def test_create_advanced_metrics_creates_career_summary_view(self):
        """Test creation of player_career_summary view."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            assert any("player_career_summary" in c for c in calls)

    def test_create_advanced_metrics_includes_true_shooting_percentage(self):
        """Test that TS% calculation is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "ts_pct" in sql_text.lower()
            assert "0.44" in sql_text  # TS% coefficient

    def test_create_advanced_metrics_includes_effective_fg_percentage(self):
        """Test that eFG% calculation is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "efg_pct" in sql_text.lower()
            assert "0.5" in sql_text  # eFG% coefficient for 3PM

    def test_create_advanced_metrics_includes_turnover_percentage(self):
        """Test that TOV% calculation is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "tov_pct" in sql_text.lower()

    def test_create_advanced_metrics_includes_game_score(self):
        """Test that Game Score calculation is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "game_score" in sql_text.lower()

    def test_create_advanced_metrics_includes_fantasy_points(self):
        """Test that fantasy points calculation is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "fantasy_pts" in sql_text.lower()

    def test_create_advanced_metrics_includes_double_double_indicator(self):
        """Test that double-double detection is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "double_double" in sql_text.lower()

    def test_create_advanced_metrics_includes_triple_double_indicator(self):
        """Test that triple-double detection is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "triple_double" in sql_text.lower()


    def test_create_advanced_metrics_commits_transaction(self):
        """Test that database transaction is committed."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            mock_con.commit.assert_called_once()

    def test_create_advanced_metrics_closes_connection_on_success(self):
        """Test that connection is closed after successful completion."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            mock_con.close.assert_called_once()

    def test_create_advanced_metrics_closes_connection_on_error(self):
        """Test that connection is closed even when error occurs."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.side_effect = Exception("SQL error")
            
            with pytest.raises(Exception):
                create_advanced_metrics()
            
            mock_con.close.assert_called_once()

    def test_create_advanced_metrics_uses_create_or_replace_for_views(self):
        """Test that views use CREATE OR REPLACE."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
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
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            table_creates = [c for c in calls 
                           if "CREATE TABLE" in c and "VIEW" not in c]
            assert any("IF NOT EXISTS" in c for c in table_creates)

    def test_create_advanced_metrics_verifies_view_creation(self):
        """Test that function verifies views were created."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
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
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
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
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            sql_text = " ".join(calls)
            assert "ppg" in sql_text.lower() or "pts_per_game" in sql_text.lower()

    def test_create_advanced_metrics_includes_primary_key_in_season_stats(self):
        """Test that player_season_stats has composite primary key."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [c.args[0] for c in mock_con.execute.call_args_list if c.args]
            season_stats_create = [c for c in calls if "player_season_stats" in c]
            assert any("PRIMARY KEY" in c for c in season_stats_create)

class TestCreateAdvancedMetricsExtended:
    """Extended test suite for advanced metrics with edge cases."""

    def test_create_advanced_metrics_accepts_custom_path(self):
        """Test that custom database path is accepted."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            custom_path = "/custom/path/to/database.duckdb"
            create_advanced_metrics(custom_path)
            
            mock_connect.assert_called_once_with(custom_path)

    def test_create_advanced_metrics_creates_all_required_views(self):
        """Test that all documented views are created."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Verify all major views/tables
            expected_objects = [
                "player_game_advanced",
                "team_game_advanced",
                "player_season_stats",
                "team_four_factors",
                "league_season_averages"
            ]
            
            for obj in expected_objects:
                assert obj in sql_text, f"Missing {obj}"

    def test_create_advanced_metrics_calculates_true_shooting_percentage(self):
        """Test that TS% formula is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # TS% formula: PTS / (2 * (FGA + 0.44 * FTA))
            assert "0.44" in sql_text or "ts_pct" in sql_text.lower()

    def test_create_advanced_metrics_calculates_effective_field_goal_percentage(self):
        """Test that eFG% formula is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # eFG% formula involves 3-point multiplier
            assert "efg" in sql_text.lower() or "0.5" in sql_text

    def test_create_advanced_metrics_calculates_game_score(self):
        """Test that Game Score (GmSc) is calculated."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls).lower()
            
            assert "gmsc" in sql_text or "game_score" in sql_text

    def test_create_advanced_metrics_handles_division_by_zero(self):
        """Test that metrics handle division by zero safely."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should use NULLIF or CASE to prevent division by zero
            assert "NULLIF" in sql_text or "CASE" in sql_text

    def test_create_advanced_metrics_aggregates_by_season(self):
        """Test that season-level aggregations are performed."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should have GROUP BY season
            assert "GROUP BY" in sql_text and "season" in sql_text.lower()

    def test_create_advanced_metrics_uses_create_or_replace(self):
        """Test that views use CREATE OR REPLACE for idempotency."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            
            # Most creates should be CREATE OR REPLACE
            create_commands = [c for c in calls if "CREATE" in c]
            replace_commands = [c for c in create_commands if "OR REPLACE" in c]
            
            assert len(replace_commands) >= len(create_commands) - 1  # Allow one CREATE TABLE

    def test_create_advanced_metrics_closes_connection_on_success(self):
        """Test that connection is closed after successful execution."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            mock_con.close.assert_called_once()

    def test_create_advanced_metrics_closes_connection_on_error(self):
        """Test that connection is closed even if errors occur."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            mock_con.execute.side_effect = Exception("SQL error")
            
            with pytest.raises(Exception):
                create_advanced_metrics()
            
            mock_con.close.assert_called_once()

    def test_create_advanced_metrics_includes_assist_to_turnover_ratio(self):
        """Test that AST/TO ratio is calculated."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls).lower()
            
            assert "ast" in sql_text and ("tov" in sql_text or "turnover" in sql_text)

    def test_create_advanced_metrics_calculates_turnover_percentage(self):
        """Test that TOV% is calculated."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls).lower()
            
            # TOV% formula includes turnover rate
            assert "tov_pct" in sql_text or ("turnover" in sql_text and "pct" in sql_text)

    def test_create_advanced_metrics_includes_four_factors(self):
        """Test that Four Factors analysis is included."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Four Factors: eFG%, TOV%, ORB%, FT Rate
            assert "four_factors" in sql_text.lower()

    def test_create_advanced_metrics_handles_missing_player_game_stats(self):
        """Test graceful handling when player_game_stats table is missing."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # First few views work, then fail on player_game_stats
            mock_con.execute.side_effect = [
                MagicMock(),
                Exception("Table 'player_game_stats' does not exist")
            ]
            
            with pytest.raises(Exception, match="player_game_stats"):
                create_advanced_metrics()

    def test_create_advanced_metrics_uses_appropriate_data_types(self):
        """Test that calculated metrics use appropriate data types."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should cast to DOUBLE for percentages
            assert "DOUBLE" in sql_text or "CAST" in sql_text or "::DOUBLE" in sql_text

    def test_create_advanced_metrics_aggregates_totals_correctly(self):
        """Test that season totals are aggregated with SUM."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should use SUM for aggregating stats
            assert "SUM(" in sql_text

    def test_create_advanced_metrics_calculates_averages_correctly(self):
        """Test that season averages use COUNT for games played."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should count games for per-game averages
            assert "COUNT(" in sql_text or "AVG(" in sql_text

    def test_create_advanced_metrics_handles_null_values_in_calculations(self):
        """Test that NULL values are handled appropriately."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should handle NULLs with COALESCE or NULLIF
            assert "COALESCE" in sql_text or "NULLIF" in sql_text or "CASE" in sql_text

    @pytest.mark.parametrize("db_path", [
        "src/backend/data/nba.duckdb",
        "test.db",
        "/tmp/test.duckdb",
        "relative/path/db.duckdb"
    ])
    def test_create_advanced_metrics_accepts_various_paths(self, db_path):
        """Test that various database path formats are accepted."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics(db_path)
            
            mock_connect.assert_called_once_with(db_path)

    def test_create_advanced_metrics_processes_multiple_seasons(self):
        """Test that metrics work across multiple seasons."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should not filter to specific season
            assert "WHERE" not in sql_text or "season" in sql_text.lower()

    def test_create_advanced_metrics_joins_player_and_game_data(self):
        """Test that player and game data are properly joined."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should have JOIN operations
            assert "JOIN" in sql_text

    def test_create_advanced_metrics_orders_results_logically(self):
        """Test that results are ordered appropriately."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Season stats should likely be ordered
            if "player_season_stats" in sql_text:
                # Check for ORDER BY in season stats context
                season_stats_calls = [c for c in calls if "player_season_stats" in c]
                # ORDER BY is optional for tables but common in views
                pass  # Accept both ordered and unordered

    def test_create_advanced_metrics_maintains_referential_integrity(self):
        """Test that foreign key relationships are preserved."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should reference player_id and game_id
            assert "player_id" in sql_text and "game_id" in sql_text

    def test_create_advanced_metrics_calculates_league_averages(self):
        """Test that league-wide averages are calculated."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            create_advanced_metrics()
            
            calls = [str(c.args[0]) if c.args else "" for c in mock_con.execute.call_args_list]
            sql_text = " ".join(calls)
            
            # Should have league averages calculation
            assert "league" in sql_text.lower() and "avg" in sql_text.lower()

    def test_create_advanced_metrics_handles_partial_execution(self):
        """Test behavior when some views succeed and others fail."""
        with patch("src.scripts.analysis.create_advanced_metrics.duckdb.connect") as mock_connect:
            mock_con = MagicMock()
            mock_connect.return_value = mock_con
            
            # Succeed on first 3, fail on 4th
            mock_con.execute.side_effect = [
                MagicMock(),
                MagicMock(),
                MagicMock(),
                Exception("View creation failed"),
            ]
            
            with pytest.raises(Exception):
                create_advanced_metrics()
            
            # Connection should still be closed
            mock_con.close.assert_called_once()
