"""Populator for NBA Win Probability data.

This populator fetches win probability play-by-play data from the NBA API,
which provides real-time win probability for each play in a game.

Data includes:
- Play-by-play actions with win probability
- Home/away win percentages at each moment
- Clutch situation tracking

Usage:
    from src.scripts.populate.populate_win_probability import WinProbabilityPopulator

    populator = WinProbabilityPopulator()
    populator.run(seasons=["2024-25"], game_ids=["0022400001"])
"""

import logging

import pandas as pd

from src.scripts.populate.base import BasePopulator
from src.scripts.populate.exceptions import DataNotFoundError, TransientError
from src.scripts.populate.helpers import configure_logging


configure_logging()
logger = logging.getLogger(__name__)


class WinProbabilityPopulator(BasePopulator):
    """Populates win probability data for NBA games.

    This endpoint provides win probability for each play, useful for:
    - Clutch analysis (games decided in final moments)
    - Momentum tracking
    - Game flow visualization
    - High-leverage play identification

    API: nba_api/stats/endpoints/winprobabilitypbp.py
    """

    def get_table_name(self) -> str:
        """Return the target table name."""
        return "win_probability"

    def get_key_columns(self) -> list[str]:
        """Return primary key columns."""
        return ["game_id", "event_num"]

    def get_data_type(self) -> str:
        """Return data type for validation."""
        return "win_probability"

    def get_expected_columns(self) -> list[str]:
        """Return expected columns for validation."""
        return [
            "game_id",
            "event_num",
            "home_pct",
            "visitor_pct",
            "home_pts",
            "visitor_pts",
            "period",
            "game_clock",
            "description",
            "location",
            "event_type",
            "player_id",
            "team_id",
        ]

    def fetch_data(self, **kwargs) -> pd.DataFrame | None:
        """Fetch win probability data for specified games.

        Args:
            **kwargs: Keyword arguments:
                - game_ids: List of game IDs to fetch (required)
                - seasons: List of seasons to process (optional, for finding games)
                - date_from: Start date for finding games (optional)
                - date_to: End date for finding games (optional)
                - resume: Whether to resume from progress checkpoint

        Returns:
            DataFrame with win probability data
        """
        game_ids = kwargs.get("game_ids", [])
        seasons = kwargs.get("seasons", [])
        date_from = kwargs.get("date_from")
        date_to = kwargs.get("date_to")
        resume = kwargs.get("resume", True)

        # If no game_ids provided, find games from seasons/dates
        if not game_ids and (seasons or (date_from and date_to)):
            game_ids = self._find_game_ids(
                seasons=seasons,
                date_from=date_from,
                date_to=date_to,
            )

        if not game_ids:
            logger.warning("No game IDs provided or found")
            return None

        all_data = []
        completed = self.progress.get_completed() if resume else set()

        for game_id in game_ids:
            if str(game_id) in completed:
                logger.debug(f"Skipping already completed game: {game_id}")
                continue

            try:
                df = self._fetch_game_win_probability(game_id)
                if df is not None and not df.empty:
                    all_data.append(df)
                    self.progress.mark_completed(str(game_id))
                    self.metrics.api_calls += 1
                else:
                    logger.warning(f"No win probability data for game {game_id}")

            except DataNotFoundError:
                logger.warning(f"Win probability not available for game {game_id}")
                self.progress.mark_completed(str(game_id))

            except TransientError as e:
                logger.exception(f"Transient error fetching game {game_id}: {e}")
                self.progress.add_error(str(game_id), str(e))

            except Exception as e:
                logger.exception(f"Error fetching win probability for {game_id}: {e}")
                self.metrics.add_error(str(e), {"game_id": game_id})
                self.progress.add_error(str(game_id), str(e))

            # Save progress periodically
            if len(all_data) % 10 == 0:
                self.progress.save()

        if not all_data:
            return None

        return pd.concat(all_data, ignore_index=True)

    def _find_game_ids(
        self,
        seasons: list[str],
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[str]:
        """Find game IDs from seasons or date range.

        Args:
            seasons: List of season strings (e.g., ["2023-24"])
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)

        Returns:
            List of game IDs
        """
        game_ids = []

        for season in seasons:
            try:
                # Use league game finder to get games
                df = self.client.get_league_game_finder(
                    player_or_team="T",
                    season=season,
                    date_from=date_from,
                    date_to=date_to,
                )

                if df is not None and not df.empty:
                    # Get unique game IDs
                    ids = df["GAME_ID"].unique().tolist()
                    game_ids.extend(ids)
                    logger.info(f"Found {len(ids)} games for season {season}")

                self.metrics.api_calls += 1

            except Exception as e:
                logger.exception(f"Error finding games for season {season}: {e}")
                self.metrics.add_error(str(e), {"season": season})

        return list(set(game_ids))

    def _fetch_game_win_probability(self, game_id: str) -> pd.DataFrame | None:
        """Fetch win probability data for a single game.

        Args:
            game_id: NBA game ID

        Returns:
            DataFrame with win probability data
        """
        try:
            from nba_api.stats.endpoints import winprobabilitypbp

            wp = winprobabilitypbp.WinProbabilityPBP(
                game_id=game_id,
                proxy=self.client.config.proxy,
                headers=self.client.config.headers,
                timeout=int(self.client.config.timeout),
            )

            # Get the win probability data - use get_data_frames() method
            data_frames = wp.get_data_frames()
            df = data_frames[0] if data_frames else pd.DataFrame()

            if df.empty:
                return None

            # Add game_id column if not present
            if "GAME_ID" not in df.columns:
                df["GAME_ID"] = game_id

            return df

        except Exception as e:
            error_str = str(e).lower()
            if "404" in error_str or "not found" in error_str:
                raise DataNotFoundError(
                    message=f"Win probability not found for game {game_id}",
                    resource_type="win_probability",
                    resource_id=game_id,
                ) from e
            if "429" in error_str or "rate" in error_str:
                raise TransientError(
                    message=f"Rate limited fetching game {game_id}",
                    retry_after=60.0,
                ) from e
            raise

    def transform_data(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Transform raw API data to match database schema.

        Args:
            df: Raw DataFrame from API
            **kwargs: Additional keyword arguments (unused)

        Returns:
            Transformed DataFrame
        """
        # Column mapping from API to database schema
        column_mapping = {
            "GAME_ID": "game_id",
            "EVENT_NUM": "event_num",
            "HOME_PCT": "home_pct",
            "VISITOR_PCT": "visitor_pct",
            "HOME_PTS": "home_pts",
            "VISITOR_PTS": "visitor_pts",
            "PERIOD": "period",
            "GAME_CLOCK": "game_clock",
            "DESCRIPTION": "description",
            "LOCATION": "location",
            "EVENTMSGTYPE": "event_type",
            "PLAYER_ID": "player_id",
            "TEAM_ID": "team_id",
            "PLAYER_NAME": "player_name",
            "HOME_TEAM_ID": "home_team_id",
            "HOME_TEAM_ABB": "home_team_abbreviation",
            "VISITOR_TEAM_ID": "visitor_team_id",
            "VISITOR_TEAM_ABB": "visitor_team_abbreviation",
        }

        # Rename columns that exist
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=rename_dict)

        # Convert percentages to decimal if needed (some come as 0-100, some as 0-1)
        if "home_pct" in df.columns and df["home_pct"].max() > 1:
            df["home_pct"] = df["home_pct"] / 100.0
        if "visitor_pct" in df.columns and df["visitor_pct"].max() > 1:
            df["visitor_pct"] = df["visitor_pct"] / 100.0

        # Ensure required columns exist
        for col in self.get_key_columns():
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        return df

    def pre_run_hook(self, **kwargs) -> None:
        """Create table if it doesn't exist."""
        conn = self.connect()

        # Check if table exists
        result = conn.execute("""
            SELECT count(*) FROM information_schema.tables
            WHERE table_name = 'win_probability_raw'
        """).fetchone()

        if result[0] == 0:
            logger.info("Creating win_probability_raw table")
            conn.execute("""
                CREATE TABLE win_probability_raw (
                    game_id VARCHAR NOT NULL,
                    event_num INTEGER NOT NULL,
                    home_pct DOUBLE,
                    visitor_pct DOUBLE,
                    home_pts INTEGER,
                    visitor_pts INTEGER,
                    period INTEGER,
                    game_clock VARCHAR,
                    description VARCHAR,
                    location VARCHAR,
                    event_type INTEGER,
                    player_id INTEGER,
                    team_id INTEGER,
                    player_name VARCHAR,
                    home_team_id INTEGER,
                    home_team_abbreviation VARCHAR,
                    visitor_team_id INTEGER,
                    visitor_team_abbreviation VARCHAR,
                    populated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (game_id, event_num)
                )
            """)
            conn.commit()


# CLI entry point
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Populate win probability data")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=["2024-25"],
        help="Seasons to process",
    )
    parser.add_argument(
        "--game-ids",
        nargs="+",
        default=[],
        help="Specific game IDs to process",
    )
    parser.add_argument(
        "--date-from",
        type=str,
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date-to",
        type=str,
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignore progress",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't insert data, just show what would be inserted",
    )

    args = parser.parse_args()

    populator = WinProbabilityPopulator()
    result = populator.run(
        seasons=args.seasons,
        game_ids=args.game_ids,
        date_from=args.date_from,
        date_to=args.date_to,
        resume=not args.no_resume,
        dry_run=args.dry_run,
    )

    print(f"\nResults: {result}")
