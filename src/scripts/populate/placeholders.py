"""Shared helpers for placeholder population scripts."""

from __future__ import annotations

import logging
from typing import Iterable, NoReturn

from src.scripts.populate.helpers import configure_logging

logger = logging.getLogger(__name__)


def raise_not_implemented(
    feature: str,
    phase: str,
    *,
    message: str | None = None,
) -> NoReturn:
    """Raise a standardized NotImplementedError for placeholder scripts."""
    configure_logging()
    logger.error("%s population not yet implemented", feature)
    logger.info("TODO: See docs/roadmap.md Phase %s for implementation requirements", phase)
    if message is None:
        message = (
            f"{feature} population is planned but not yet implemented. "
            f"See docs/roadmap.md Phase {phase} for requirements."
        )
    raise NotImplementedError(message)


def log_placeholder_banner(
    feature: str,
    phase: str,
    *,
    status: str | None = None,
    decisions: Iterable[str] | None = None,
) -> None:
    """Emit a consistent placeholder banner for unimplemented scripts."""
    configure_logging()
    title = f"{feature.upper()} POPULATION - NOT YET IMPLEMENTED"
    logger.warning("=" * 70)
    logger.warning(title)
    logger.warning("=" * 70)
    logger.warning("This script is a placeholder for future development.")
    logger.warning("See docs/roadmap.md Phase %s for implementation requirements.", phase)
    if status:
        logger.warning("")
        logger.warning(status)
    if decisions:
        logger.warning("")
        logger.warning("Key Decisions Needed:")
        for idx, item in enumerate(decisions, start=1):
            logger.warning("%s. %s", idx, item)
    logger.warning("=" * 70)
