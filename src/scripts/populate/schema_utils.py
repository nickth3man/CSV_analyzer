"""Schema helpers for populate scripts."""

from __future__ import annotations

import logging
from typing import Sequence

import duckdb

from src.scripts.populate.config import PLAY_BY_PLAY_FIELDS

logger = logging.getLogger(__name__)

PLAY_BY_PLAY_COLUMNS: Sequence[str] = PLAY_BY_PLAY_FIELDS

PLAY_BY_PLAY_SCHEMA_SQL = """
    CREATE TABLE IF NOT EXISTS play_by_play (
        game_id BIGINT NOT NULL,
        action_number BIGINT NOT NULL,
        clock VARCHAR,
        period INTEGER,
        team_id BIGINT,
        team_tricode VARCHAR,
        person_id BIGINT,
        player_name VARCHAR,
        player_name_i VARCHAR,
        x_legacy DOUBLE,
        y_legacy DOUBLE,
        shot_distance DOUBLE,
        shot_result VARCHAR,
        is_field_goal INTEGER,
        score_home VARCHAR,
        score_away VARCHAR,
        points_total INTEGER,
        location VARCHAR,
        description VARCHAR,
        action_type VARCHAR,
        sub_type VARCHAR,
        video_available INTEGER,
        shot_value INTEGER,
        action_id INTEGER,
        PRIMARY KEY (game_id, action_number)
    )
"""


def ensure_play_by_play_schema(
    conn: duckdb.DuckDBPyConnection,
    *,
    force: bool = False,
    drop_if_mismatch: bool = False,
) -> str | None:
    """Ensure play_by_play matches the expected schema, recreating if allowed."""
    try:
        cols = conn.execute("PRAGMA table_info('play_by_play')").fetchall()
    except duckdb.CatalogException:
        cols = []

    if not cols:
        conn.execute(PLAY_BY_PLAY_SCHEMA_SQL)
        return "created"

    existing = [col[1] for col in cols]
    pk_cols = [
        col[1]
        for col in sorted(
            ((col[5], col[1]) for col in cols if col[5]),
            key=lambda item: item[0],
        )
    ]

    if existing == list(PLAY_BY_PLAY_COLUMNS) and pk_cols == ["game_id", "action_number"]:
        return "ok"

    if drop_if_mismatch:
        logger.warning("Recreating play_by_play schema due to mismatch.")
        conn.execute("DROP TABLE IF EXISTS play_by_play")
        conn.execute(PLAY_BY_PLAY_SCHEMA_SQL)
        return "recreated"

    row_count = conn.execute("SELECT COUNT(*) FROM play_by_play").fetchone()[0]
    if row_count == 0 or force:
        logger.info("Recreating play_by_play schema due to mismatch.")
        conn.execute("DROP TABLE IF EXISTS play_by_play")
        conn.execute(PLAY_BY_PLAY_SCHEMA_SQL)
        return "recreated"

    logger.warning("play_by_play schema mismatch with existing data; skipping.")
    return "mismatch"
