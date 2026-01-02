"""Unit tests for src.scripts.populate.database module.

Tests DatabaseManager class including bulk_upsert, bulk_insert,
and connection management.
"""

import pandas as pd
import pytest


@pytest.fixture
def temp_db_path(tmp_path):
    """Create a temporary database path for testing."""
    return tmp_path / "test_nba.duckdb"


@pytest.fixture
def db_manager(temp_db_path):
    """Create a DatabaseManager instance with temporary database."""
    from src.scripts.populate.database import DatabaseManager

    manager = DatabaseManager(db_path=temp_db_path)
    yield manager
    manager.close()


@pytest.fixture
def db_with_test_table(db_manager):
    """Create a database manager with a test table."""
    conn = db_manager.connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_players (
            player_id INTEGER PRIMARY KEY,
            player_name VARCHAR,
            team_id INTEGER,
            points INTEGER
        )
    """)
    conn.commit()
    return db_manager


@pytest.fixture
def db_with_composite_key_table(db_manager):
    """Create a database manager with a composite key test table."""
    conn = db_manager.connect()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_game_stats (
            game_id VARCHAR,
            player_id INTEGER,
            team_id INTEGER,
            points INTEGER,
            assists INTEGER,
            rebounds INTEGER,
            PRIMARY KEY (game_id, player_id)
        )
    """)
    conn.commit()
    return db_manager


class TestDatabaseManagerConnection:
    """Tests for DatabaseManager connection management."""

    def test_connect_creates_connection(self, db_manager):
        """Test connect() creates a database connection."""
        conn = db_manager.connect()
        assert conn is not None

    def test_connect_returns_same_connection(self, db_manager):
        """Test connect() returns the same connection on multiple calls."""
        conn1 = db_manager.connect()
        conn2 = db_manager.connect()
        assert conn1 is conn2

    def test_close_closes_connection(self, db_manager):
        """Test close() properly closes the connection."""
        db_manager.connect()
        assert db_manager.connection is not None
        db_manager.close()
        assert db_manager.connection is None

    def test_context_manager(self, temp_db_path):
        """Test DatabaseManager works as context manager."""
        from src.scripts.populate.database import DatabaseManager

        with DatabaseManager(db_path=temp_db_path) as conn:
            assert conn is not None
            # Should be able to execute queries
            result = conn.execute("SELECT 1").fetchone()
            assert result[0] == 1


class TestBulkInsert:
    """Tests for bulk_insert method."""

    def test_bulk_insert_empty_dataframe(self, db_with_test_table):
        """Test bulk_insert handles empty DataFrame gracefully."""
        empty_df = pd.DataFrame(
            columns=["player_id", "player_name", "team_id", "points"]
        )
        rows = db_with_test_table.bulk_insert(empty_df, "test_players")
        assert rows == 0

    def test_bulk_insert_single_row(self, db_with_test_table):
        """Test bulk_insert with single row."""
        df = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Test Player"],
                "team_id": [100],
                "points": [25],
            }
        )
        rows = db_with_test_table.bulk_insert(df, "test_players")
        assert rows == 1

        # Verify data was inserted
        conn = db_with_test_table.connect()
        result = conn.execute(
            "SELECT * FROM test_players WHERE player_id = 1"
        ).fetchone()
        assert result[0] == 1
        assert result[1] == "Test Player"

    def test_bulk_insert_multiple_rows(self, db_with_test_table):
        """Test bulk_insert with multiple rows."""
        df = pd.DataFrame(
            {
                "player_id": [1, 2, 3],
                "player_name": ["Player A", "Player B", "Player C"],
                "team_id": [100, 100, 200],
                "points": [20, 30, 15],
            }
        )
        rows = db_with_test_table.bulk_insert(df, "test_players")
        assert rows == 3

        # Verify count
        conn = db_with_test_table.connect()
        count = conn.execute("SELECT COUNT(*) FROM test_players").fetchone()[0]
        assert count == 3

    def test_bulk_insert_large_dataset(self, db_with_test_table):
        """Test bulk_insert with larger dataset (1000 rows)."""
        df = pd.DataFrame(
            {
                "player_id": range(1, 1001),
                "player_name": [f"Player {i}" for i in range(1, 1001)],
                "team_id": [i % 30 for i in range(1, 1001)],
                "points": [i % 50 for i in range(1, 1001)],
            }
        )
        rows = db_with_test_table.bulk_insert(df, "test_players")
        assert rows == 1000

        # Verify count
        conn = db_with_test_table.connect()
        count = conn.execute("SELECT COUNT(*) FROM test_players").fetchone()[0]
        assert count == 1000


class TestBulkUpsert:
    """Tests for bulk_upsert method with MERGE statement."""

    def test_bulk_upsert_empty_dataframe(self, db_with_test_table):
        """Test bulk_upsert handles empty DataFrame gracefully."""
        empty_df = pd.DataFrame(
            columns=["player_id", "player_name", "team_id", "points"]
        )
        rows = db_with_test_table.bulk_upsert(empty_df, "test_players", ["player_id"])
        assert rows == 0

    def test_bulk_upsert_insert_new_rows(self, db_with_test_table):
        """Test bulk_upsert inserts new rows when table is empty."""
        df = pd.DataFrame(
            {
                "player_id": [1, 2, 3],
                "player_name": ["Player A", "Player B", "Player C"],
                "team_id": [100, 100, 200],
                "points": [20, 30, 15],
            }
        )
        rows = db_with_test_table.bulk_upsert(df, "test_players", ["player_id"])
        assert rows == 3

        # Verify data
        conn = db_with_test_table.connect()
        count = conn.execute("SELECT COUNT(*) FROM test_players").fetchone()[0]
        assert count == 3

    def test_bulk_upsert_updates_existing_rows(self, db_with_test_table):
        """Test bulk_upsert updates existing rows."""
        # Insert initial data
        initial_df = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_name": ["Player A", "Player B"],
                "team_id": [100, 100],
                "points": [20, 30],
            }
        )
        db_with_test_table.bulk_insert(initial_df, "test_players")

        # Upsert with updated values
        updated_df = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_name": ["Player A Updated", "Player B Updated"],
                "team_id": [200, 200],
                "points": [50, 60],
            }
        )
        rows = db_with_test_table.bulk_upsert(updated_df, "test_players", ["player_id"])
        assert rows == 2

        # Verify updates
        conn = db_with_test_table.connect()
        result = conn.execute(
            "SELECT player_name, points FROM test_players WHERE player_id = 1"
        ).fetchone()
        assert result[0] == "Player A Updated"
        assert result[1] == 50

    def test_bulk_upsert_mixed_insert_update(self, db_with_test_table):
        """Test bulk_upsert handles mix of inserts and updates."""
        # Insert initial data
        initial_df = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_name": ["Player A", "Player B"],
                "team_id": [100, 100],
                "points": [20, 30],
            }
        )
        db_with_test_table.bulk_insert(initial_df, "test_players")

        # Upsert: update player 1, insert player 3
        mixed_df = pd.DataFrame(
            {
                "player_id": [1, 3],
                "player_name": ["Player A Updated", "Player C New"],
                "team_id": [200, 300],
                "points": [50, 25],
            }
        )
        rows = db_with_test_table.bulk_upsert(mixed_df, "test_players", ["player_id"])
        assert rows == 2

        # Verify results
        conn = db_with_test_table.connect()

        # Player 1 should be updated
        p1 = conn.execute(
            "SELECT player_name, points FROM test_players WHERE player_id = 1"
        ).fetchone()
        assert p1[0] == "Player A Updated"
        assert p1[1] == 50

        # Player 2 should be unchanged
        p2 = conn.execute(
            "SELECT player_name, points FROM test_players WHERE player_id = 2"
        ).fetchone()
        assert p2[0] == "Player B"
        assert p2[1] == 30

        # Player 3 should be inserted
        p3 = conn.execute(
            "SELECT player_name, points FROM test_players WHERE player_id = 3"
        ).fetchone()
        assert p3[0] == "Player C New"
        assert p3[1] == 25

        # Total should be 3
        count = conn.execute("SELECT COUNT(*) FROM test_players").fetchone()[0]
        assert count == 3

    def test_bulk_upsert_composite_key(self, db_with_composite_key_table):
        """Test bulk_upsert with composite primary key."""
        # Insert initial data
        initial_df = pd.DataFrame(
            {
                "game_id": ["G001", "G001"],
                "player_id": [1, 2],
                "team_id": [100, 100],
                "points": [20, 30],
                "assists": [5, 8],
                "rebounds": [10, 5],
            }
        )
        db_with_composite_key_table.bulk_insert(initial_df, "test_game_stats")

        # Upsert: update (G001, 1), insert (G002, 1)
        mixed_df = pd.DataFrame(
            {
                "game_id": ["G001", "G002"],
                "player_id": [1, 1],
                "team_id": [100, 100],
                "points": [35, 28],
                "assists": [10, 7],
                "rebounds": [12, 8],
            }
        )
        rows = db_with_composite_key_table.bulk_upsert(
            mixed_df, "test_game_stats", ["game_id", "player_id"]
        )
        assert rows == 2

        # Verify
        conn = db_with_composite_key_table.connect()

        # (G001, 1) should be updated
        g1p1 = conn.execute(
            "SELECT points, assists FROM test_game_stats WHERE game_id = 'G001' AND player_id = 1"
        ).fetchone()
        assert g1p1[0] == 35
        assert g1p1[1] == 10

        # (G001, 2) should be unchanged
        g1p2 = conn.execute(
            "SELECT points, assists FROM test_game_stats WHERE game_id = 'G001' AND player_id = 2"
        ).fetchone()
        assert g1p2[0] == 30
        assert g1p2[1] == 8

        # (G002, 1) should be inserted
        g2p1 = conn.execute(
            "SELECT points, assists FROM test_game_stats WHERE game_id = 'G002' AND player_id = 1"
        ).fetchone()
        assert g2p1[0] == 28
        assert g2p1[1] == 7


class TestUpsertData:
    """Tests for the original upsert_data method."""

    def test_upsert_data_empty_dataframe(self, db_with_test_table):
        """Test upsert_data handles empty DataFrame."""
        empty_df = pd.DataFrame(
            columns=["player_id", "player_name", "team_id", "points"]
        )
        rows = db_with_test_table.upsert_data("test_players", empty_df, ["player_id"])
        assert rows == 0

    def test_upsert_data_insert(self, db_with_test_table):
        """Test upsert_data inserts new data."""
        df = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_name": ["Player A", "Player B"],
                "team_id": [100, 100],
                "points": [20, 30],
            }
        )
        rows = db_with_test_table.upsert_data("test_players", df, ["player_id"])
        assert rows == 2


class TestInsertData:
    """Tests for the insert_data method."""

    def test_insert_data_append_mode(self, db_with_test_table):
        """Test insert_data in append mode."""
        df = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_name": ["Player A", "Player B"],
                "team_id": [100, 100],
                "points": [20, 30],
            }
        )
        rows = db_with_test_table.insert_data("test_players", df, mode="append")
        assert rows == 2

    def test_insert_data_empty_dataframe(self, db_with_test_table):
        """Test insert_data with empty DataFrame returns 0."""
        empty_df = pd.DataFrame(
            columns=["player_id", "player_name", "team_id", "points"]
        )
        rows = db_with_test_table.insert_data("test_players", empty_df)
        assert rows == 0


class TestDatabaseSchema:
    """Tests for schema creation."""

    def test_create_schema(self, db_manager):
        """Test create_schema creates all expected tables."""
        db_manager.create_schema()

        conn = db_manager.connect()
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()
        table_names = {t[0] for t in tables}

        expected_tables = {
            "players",
            "teams",
            "games",
            "player_game_stats",
            "team_game_stats",
            "boxscores",
            "play_by_play",
            "shot_charts",
            "standings",
            "tracking_stats",
            "hustle_stats",
        }
        for table in expected_tables:
            assert table in table_names, f"Table {table} not created"


class TestGetTableInfo:
    """Tests for get_table_info method."""

    def test_get_table_info(self, db_with_test_table):
        """Test get_table_info returns expected structure."""
        # Insert some data first
        df = pd.DataFrame(
            {
                "player_id": [1, 2],
                "player_name": ["Player A", "Player B"],
                "team_id": [100, 100],
                "points": [20, 30],
            }
        )
        db_with_test_table.bulk_insert(df, "test_players")

        info = db_with_test_table.get_table_info("test_players")

        assert info["table_name"] == "test_players"
        assert info["row_count"] == 2
        assert "player_id" in info["columns"]
        assert "player_name" in info["columns"]


class TestGetDatabaseStats:
    """Tests for get_database_stats method."""

    def test_get_database_stats(self, db_with_test_table):
        """Test get_database_stats returns database overview."""
        # Insert some data
        df = pd.DataFrame(
            {
                "player_id": [1],
                "player_name": ["Player A"],
                "team_id": [100],
                "points": [20],
            }
        )
        db_with_test_table.bulk_insert(df, "test_players")

        stats = db_with_test_table.get_database_stats()

        assert "database_path" in stats
        assert "table_count" in stats
        assert "total_rows" in stats
        assert "tables" in stats
        assert stats["total_rows"] >= 1
