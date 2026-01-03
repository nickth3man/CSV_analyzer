"""Shared data transformation helpers for population scripts.

This module provides reusable DataFrame transformation utilities for NBA data
population scripts, including:

- Column operations (renaming, ensuring, selecting)
- Type coercion (nullable integers, floats, dates)
- Data normalization (minutes parsing, ID extraction)
- Bulk transformation pipelines

Usage:
    from src.scripts.populate.transform_utils import (
        transform_dataframe,
        coerce_to_nullable_int,
        parse_minutes,
    )
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

import pandas as pd

from src.scripts.populate.constants import (
    FLOAT_STAT_COLUMNS,
    INTEGER_STAT_COLUMNS,
    PERCENTAGE_COLUMNS,
    ColumnMapping,
)


if TYPE_CHECKING:
    from pandas import Series


# Type aliases for clarity
ColumnRenameMap = dict[str, str]
TransformFunc = Callable[[pd.DataFrame], pd.DataFrame]
T = TypeVar("T", bound=pd.DataFrame)


# =============================================================================
# COLUMN OPERATIONS
# =============================================================================


def rename_columns(
    df: pd.DataFrame,
    mapping: ColumnRenameMap | type[ColumnMapping],
    *,
    errors: str = "ignore",
) -> pd.DataFrame:
    """Rename DataFrame columns using a mapping dictionary or ColumnMapping class.

    Args:
        df: Input DataFrame to rename columns.
        mapping: Either a dict of {old_name: new_name} or a ColumnMapping subclass.
        errors: How to handle missing columns. "ignore" skips missing, "raise" errors.

    Returns:
        DataFrame with renamed columns.

    Example:
        >>> df = rename_columns(df, PlayerGameLogColumnMap)
        >>> df = rename_columns(df, {"GAME_ID": "game_id", "PTS": "pts"})
    """
    if isinstance(mapping, type) and issubclass(mapping, ColumnMapping):
        rename_map = mapping.MAPPING
    else:
        rename_map = mapping

    return df.rename(columns=rename_map, errors=errors)


def ensure_columns(
    df: pd.DataFrame,
    columns: Sequence[str],
    *,
    fill_value: Any = None,
    dtype: str | None = None,
) -> pd.DataFrame:
    """Ensure a DataFrame includes the specified columns.

    Missing columns are added with the specified fill value and optional dtype.

    Args:
        df: Input DataFrame.
        columns: List of column names that must exist.
        fill_value: Default value for missing columns (default: None).
        dtype: Optional dtype for new columns (e.g., "Int64", "float64").

    Returns:
        DataFrame with all specified columns present.

    Example:
        >>> df = ensure_columns(df, ["game_id", "pts", "ast"], fill_value=0)
    """
    for column in columns:
        if column not in df.columns:
            if dtype:
                df[column] = pd.Series([fill_value] * len(df), dtype=dtype)
            else:
                df[column] = fill_value
    return df


def select_columns(
    df: pd.DataFrame,
    columns: Sequence[str],
    *,
    strict: bool = False,
) -> pd.DataFrame:
    """Select only specified columns from DataFrame.

    Args:
        df: Input DataFrame.
        columns: List of column names to keep.
        strict: If True, raise KeyError for missing columns. If False, skip missing.

    Returns:
        DataFrame with only the specified columns (that exist).

    Example:
        >>> df = select_columns(df, ["game_id", "player_id", "pts"])
    """
    if strict:
        return df[list(columns)]

    existing = [col for col in columns if col in df.columns]
    return df[existing]


def drop_columns(
    df: pd.DataFrame,
    columns: Sequence[str],
    *,
    errors: str = "ignore",
) -> pd.DataFrame:
    """Drop specified columns from DataFrame.

    Args:
        df: Input DataFrame.
        columns: List of column names to drop.
        errors: How to handle missing columns ("ignore" or "raise").

    Returns:
        DataFrame with specified columns removed.
    """
    return df.drop(columns=list(columns), errors=errors)


# =============================================================================
# TYPE COERCION
# =============================================================================


def coerce_to_nullable_int(
    series: Series[Any],
    *,
    errors: str = "coerce",
) -> Series[Any]:
    """Convert a series to nullable Int64 dtype.

    Handles NaN values properly using pandas nullable integer type.

    Args:
        series: Input pandas Series.
        errors: How to handle conversion errors ("coerce", "raise", "ignore").

    Returns:
        Series with Int64 dtype (nullable integer).

    Example:
        >>> df["game_id"] = coerce_to_nullable_int(df["GAME_ID"])
    """
    return pd.to_numeric(series, errors=errors).astype("Int64")


def coerce_to_float(
    series: Series[Any],
    *,
    errors: str = "coerce",
) -> Series[Any]:
    """Convert a series to float64 dtype.

    Args:
        series: Input pandas Series.
        errors: How to handle conversion errors ("coerce", "raise", "ignore").

    Returns:
        Series with float64 dtype.
    """
    return pd.to_numeric(series, errors=errors).astype("float64")


def coerce_integer_columns(
    df: pd.DataFrame,
    columns: Sequence[str] | None = None,
    *,
    errors: str = "coerce",
) -> pd.DataFrame:
    """Coerce multiple columns to nullable Int64.

    If columns is None, uses INTEGER_STAT_COLUMNS from constants.

    Args:
        df: Input DataFrame.
        columns: Columns to coerce, or None to use defaults.
        errors: How to handle conversion errors.

    Returns:
        DataFrame with specified columns as Int64.
    """
    target_cols = columns if columns is not None else INTEGER_STAT_COLUMNS
    for col in target_cols:
        if col in df.columns:
            df[col] = coerce_to_nullable_int(df[col], errors=errors)
    return df


def coerce_float_columns(
    df: pd.DataFrame,
    columns: Sequence[str] | None = None,
    *,
    errors: str = "coerce",
) -> pd.DataFrame:
    """Coerce multiple columns to float64.

    If columns is None, uses FLOAT_STAT_COLUMNS and PERCENTAGE_COLUMNS from constants.

    Args:
        df: Input DataFrame.
        columns: Columns to coerce, or None to use defaults.
        errors: How to handle conversion errors.

    Returns:
        DataFrame with specified columns as float64.
    """
    target_cols = (
        columns
        if columns is not None
        else list(FLOAT_STAT_COLUMNS | PERCENTAGE_COLUMNS)
    )
    for col in target_cols:
        if col in df.columns:
            df[col] = coerce_to_float(df[col], errors=errors)
    return df


def coerce_id_columns(
    df: pd.DataFrame,
    id_columns: Sequence[str] = ("game_id", "team_id", "player_id", "person_id"),
    *,
    errors: str = "coerce",
) -> pd.DataFrame:
    """Coerce ID columns to nullable Int64.

    Args:
        df: Input DataFrame.
        id_columns: List of ID column names to coerce.
        errors: How to handle conversion errors.

    Returns:
        DataFrame with ID columns as Int64.
    """
    for col in id_columns:
        if col in df.columns:
            df[col] = coerce_to_nullable_int(df[col], errors=errors)
    return df


# =============================================================================
# DATA PARSING & NORMALIZATION
# =============================================================================


def parse_minutes(value: Any) -> str | None:
    """Normalize minutes from numeric or string values.

    Handles various formats from NBA API:
    - Integer (e.g., 35)
    - Float (e.g., 35.5)
    - String (e.g., "35:30", "PT35M30.00S")

    Args:
        value: Raw minutes value in any format.

    Returns:
        Normalized minutes string, or None if invalid/missing.

    Example:
        >>> parse_minutes(35)
        '35'
        >>> parse_minutes(35.5)
        '35'
        >>> parse_minutes(None)
        None
    """
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, (int, float)):
        return str(int(value))
    return str(value)


def extract_game_id(value: Any) -> int | None:
    """Extract numeric game_id from various formats.

    Handles:
    - Integer (returns as-is)
    - String numeric (e.g., "0022400123")
    - None/NaN (returns None)

    Args:
        value: Raw game_id value.

    Returns:
        Integer game_id or None.
    """
    if pd.isna(value) or value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def normalize_boolean(
    value: Any, *, true_values: tuple[Any, ...] = (1, "1", True, "Y", "Yes")
) -> bool | None:
    """Normalize various boolean representations to Python bool.

    Args:
        value: Raw value that may represent a boolean.
        true_values: Tuple of values considered True.

    Returns:
        True, False, or None for null values.
    """
    if pd.isna(value) or value is None:
        return None
    return value in true_values


def parse_date(value: Any, *, format_str: str | None = None) -> pd.Timestamp | None:
    """Parse date string to pandas Timestamp.

    Args:
        value: Raw date value (string or datetime-like).
        format_str: Optional strftime format string for parsing.

    Returns:
        pandas Timestamp or None if parsing fails.
    """
    if pd.isna(value) or value is None:
        return None
    try:
        if format_str:
            return pd.to_datetime(value, format=format_str)
        return pd.to_datetime(value)
    except (ValueError, TypeError):
        return None


# =============================================================================
# TRANSFORMATION PIPELINE
# =============================================================================


def transform_dataframe(
    df: pd.DataFrame,
    *,
    rename: ColumnRenameMap | type[ColumnMapping] | None = None,
    ensure: Sequence[str] | None = None,
    select: Sequence[str] | None = None,
    drop: Sequence[str] | None = None,
    coerce_ints: Sequence[str] | bool = False,
    coerce_floats: Sequence[str] | bool = False,
    coerce_ids: bool = True,
    custom_transforms: Sequence[TransformFunc] | None = None,
) -> pd.DataFrame:
    """Apply a series of transformations to a DataFrame in a single call.

    This is a convenience function that chains common transformations.
    Operations are applied in this order:
    1. Rename columns
    2. Ensure columns exist
    3. Coerce ID columns
    4. Coerce integer columns
    5. Coerce float columns
    6. Apply custom transforms
    7. Select columns (drop others)
    8. Drop specified columns

    Args:
        df: Input DataFrame to transform.
        rename: Column rename mapping (dict or ColumnMapping class).
        ensure: Columns to ensure exist with None default.
        select: Columns to keep (drops all others).
        drop: Columns to explicitly drop.
        coerce_ints: Columns to coerce to Int64, or True for defaults.
        coerce_floats: Columns to coerce to float64, or True for defaults.
        coerce_ids: Whether to coerce standard ID columns (default: True).
        custom_transforms: List of functions to apply to DataFrame.

    Returns:
        Transformed DataFrame.

    Example:
        >>> df = transform_dataframe(
        ...     raw_df,
        ...     rename=PlayerGameLogColumnMap,
        ...     ensure=PLAYER_GAME_STATS_COLUMNS,
        ...     coerce_ints=True,
        ...     coerce_ids=True,
        ... )
    """
    # 1. Rename columns
    if rename:
        df = rename_columns(df, rename)

    # 2. Ensure columns exist
    if ensure:
        df = ensure_columns(df, ensure)

    # 3. Coerce ID columns
    if coerce_ids:
        df = coerce_id_columns(df)

    # 4. Coerce integer columns
    if coerce_ints:
        cols = None if coerce_ints is True else list(coerce_ints)
        df = coerce_integer_columns(df, cols)

    # 5. Coerce float columns
    if coerce_floats:
        cols = None if coerce_floats is True else list(coerce_floats)
        df = coerce_float_columns(df, cols)

    # 6. Apply custom transforms
    if custom_transforms:
        for transform_fn in custom_transforms:
            df = transform_fn(df)

    # 7. Select columns
    if select:
        df = select_columns(df, select)

    # 8. Drop columns
    if drop:
        df = drop_columns(df, drop)

    return df


def apply_column_mapping(
    df: pd.DataFrame,
    mapping: Mapping[str, tuple[str, Callable[[Any], Any] | None]],
) -> pd.DataFrame:
    """Apply column-specific transformations with renaming.

    Each mapping entry specifies the new column name and an optional
    transformation function.

    Args:
        df: Input DataFrame.
        mapping: Dict of {api_col: (db_col, transform_fn or None)}.

    Returns:
        DataFrame with columns renamed and optionally transformed.

    Example:
        >>> mapping = {
        ...     "GAME_ID": ("game_id", coerce_to_nullable_int),
        ...     "PTS": ("pts", None),  # Just rename
        ...     "MIN": ("min", parse_minutes),
        ... }
        >>> df = apply_column_mapping(df, mapping)
    """
    result = pd.DataFrame()

    for api_col, (db_col, transform_fn) in mapping.items():
        if api_col in df.columns:
            if transform_fn:
                result[db_col] = df[api_col].apply(transform_fn)
            else:
                result[db_col] = df[api_col]

    return result


# =============================================================================
# SPECIALIZED TRANSFORMS
# =============================================================================


def create_empty_stats_df(
    columns: Sequence[str],
    *,
    int_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Create an empty DataFrame with proper column dtypes for stats.

    Args:
        columns: List of column names.
        int_columns: Columns that should be Int64 dtype.

    Returns:
        Empty DataFrame with appropriate column dtypes.
    """
    df = pd.DataFrame(columns=list(columns))

    if int_columns:
        for col in int_columns:
            if col in df.columns:
                df[col] = pd.Series(dtype="Int64")

    return df


def pivot_home_away(
    df: pd.DataFrame,
    *,
    home_map: dict[str, str],
    away_map: dict[str, str],
    common_map: dict[str, str],
    merge_key: str = "game_id",
) -> pd.DataFrame:
    """Pivot team-level data into home/away format for game table.

    Args:
        df: DataFrame with one row per team per game.
        home_map: Column rename map for home team (e.g., "PTS" -> "pts_home").
        away_map: Column rename map for away team (e.g., "PTS" -> "pts_away").
        common_map: Column rename map for common fields (e.g., "GAME_DATE" -> "game_date").
        merge_key: Column to merge home and away on.

    Returns:
        DataFrame with one row per game, home and away columns.
    """
    # Identify home vs away based on MATCHUP containing "vs."
    is_home = df["MATCHUP"].str.contains("vs.", na=False)

    # Split into home and away
    home_df = df[is_home].rename(columns={**common_map, **home_map})
    away_df = df[~is_home].rename(columns={**common_map, **away_map})

    # Keep only relevant columns from away (drop common columns except merge key)
    away_cols = [merge_key] + [c for c in away_df.columns if c.endswith("_away")]
    away_df = away_df[away_cols]

    # Merge home and away
    return home_df.merge(away_df, on=merge_key, how="inner")
