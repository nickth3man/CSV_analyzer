"""Conversation memory for the NBA Data Analyst Agent.

This module tracks conversation context for follow-up questions,
as specified in design.md Section 4.4.
"""

from __future__ import annotations

import logging
import re
import threading
from collections import deque

from src.backend.models import ConversationContext, ConversationTurn, ResolvedReferences


logger = logging.getLogger(__name__)

MAX_HISTORY_SIZE = 20


class ConversationMemory:
    """Tracks conversation context for natural follow-up questions.

    Stores recent Q&A turns and resolves pronouns/references
    using conversation history.
    """

    def __init__(self, max_turns: int = MAX_HISTORY_SIZE) -> None:
        """Initialize conversation memory.

        Args:
            max_turns: Maximum number of turns to retain.
        """
        self.max_turns = max_turns
        self._turns: deque[ConversationTurn] = deque(maxlen=max_turns)
        self._lock = threading.Lock()
        self._last_tables: list[str] = []
        self._last_sql: str | None = None

    def add_turn(
        self,
        question: str,
        answer: str,
        sql: str | None = None,
        rewritten_query: str | None = None,
        tables_used: list[str] | None = None,
    ) -> None:
        """Store a Q&A turn in memory.

        Args:
            question: The user's question.
            answer: The system's answer.
            sql: The SQL query used (if any).
            rewritten_query: The rewritten/normalized query.
            tables_used: List of tables used in the query.
        """
        with self._lock:
            turn = ConversationTurn(
                question=question,
                rewritten_query=rewritten_query,
                sql=sql,
                answer=answer,
            )
            self._turns.append(turn)

            if sql:
                self._last_sql = sql
            if tables_used:
                self._last_tables = tables_used

    def get_context(self, n_turns: int = 3) -> ConversationContext:
        """Retrieve the last n turns of conversation.

        Args:
            n_turns: Number of recent turns to retrieve.

        Returns:
            ConversationContext with recent history.
        """
        with self._lock:
            recent = list(self._turns)[-n_turns:]
            return ConversationContext(
                turns=recent,
                last_tables_used=self._last_tables.copy(),
                last_sql=self._last_sql,
            )

    def extract_references(self, query: str) -> ResolvedReferences:
        """Resolve pronouns and references in a query.

        Analyzes the query for references like "they", "that team",
        "same stats" and resolves them using conversation history.

        Args:
            query: The user's query with potential references.

        Returns:
            ResolvedReferences with expanded query.
        """
        with self._lock:
            if not self._turns:
                return ResolvedReferences(
                    original_query=query,
                    expanded_query=query,
                )

            last_turn = self._turns[-1] if self._turns else None
            last_entities = self._extract_entities_from_turn(last_turn)
            resolved_entities: dict[str, str] = {}

            resolved_entities.update(
                self._resolve_pronouns(query, last_entities)
            )
            resolved_entities.update(
                self._resolve_references(query, last_entities, last_turn)
            )

            expanded_query = self._apply_resolutions(query, resolved_entities)

            return ResolvedReferences(
                original_query=query,
                expanded_query=expanded_query,
                resolved_entities=resolved_entities,
            )

    def _resolve_pronouns(
        self, query: str, last_entities: dict[str, str]
    ) -> dict[str, str]:
        pronoun_patterns = [
            (r"\bthey\b", "team_or_player"),
            (r"\bthem\b", "team_or_player"),
            (r"\btheir\b", "team_or_player"),
            (r"\bhe\b", "player"),
            (r"\bhim\b", "player"),
            (r"\bhis\b", "player"),
            (r"\bshe\b", "player"),
            (r"\bher\b", "player"),
            (r"\bit\b", "team"),
        ]
        resolved: dict[str, str] = {}
        for pattern, entity_type in pronoun_patterns:
            if not re.search(pattern, query, re.IGNORECASE):
                continue
            resolved_value = self._resolve_entity_type(entity_type, last_entities)
            if resolved_value:
                resolved[pattern] = resolved_value
        return resolved

    def _resolve_entity_type(
        self, entity_type: str, last_entities: dict[str, str]
    ) -> str | None:
        if entity_type == "team_or_player":
            return last_entities.get("team") or last_entities.get("player")
        return last_entities.get(entity_type)

    def _resolve_references(
        self,
        query: str,
        last_entities: dict[str, str],
        last_turn: ConversationTurn | None,
    ) -> dict[str, str]:
        reference_patterns = [
            (r"\bthat team\b", "team"),
            (r"\bthe team\b", "team"),
            (r"\bthat player\b", "player"),
            (r"\bthe player\b", "player"),
            (r"\bsame stats?\b", "stats"),
            (r"\bsame time(?:frame|period)?\b", "timeframe"),
            (r"\bhow about\b", "comparison"),
            (r"\bwhat about\b", "comparison"),
        ]
        resolved: dict[str, str] = {}
        for pattern, ref_type in reference_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if not match:
                continue
            resolved_value = self._resolve_reference_type(
                ref_type, last_entities, last_turn
            )
            if resolved_value:
                resolved[match.group()] = resolved_value
        return resolved

    def _resolve_reference_type(
        self,
        ref_type: str,
        last_entities: dict[str, str],
        last_turn: ConversationTurn | None,
    ) -> str | None:
        if ref_type in {"team", "player"}:
            return last_entities.get(ref_type)
        if ref_type == "comparison" and last_turn:
            return self._build_comparison_context(last_turn)
        return None

    def _apply_resolutions(
        self, query: str, resolved_entities: dict[str, str]
    ) -> str:
        expanded_query = query
        for ref, resolved in resolved_entities.items():
            if ref.startswith("\\b"):
                expanded_query = re.sub(
                    ref, resolved, expanded_query, flags=re.IGNORECASE
                )
            else:
                expanded_query = expanded_query.replace(ref, resolved)
        return expanded_query

    def _extract_entities_from_turn(
        self, turn: ConversationTurn | None
    ) -> dict[str, str]:
        """Extract entities (teams, players) from a conversation turn.

        Args:
            turn: The conversation turn to analyze.

        Returns:
            Dictionary of entity types to values.
        """
        if not turn:
            return {}

        entities: dict[str, str] = {}

        text = f"{turn.question} {turn.rewritten_query or ''} {turn.answer or ''}"

        nba_teams = [
            "Lakers",
            "Celtics",
            "Warriors",
            "Bulls",
            "Heat",
            "Nets",
            "Knicks",
            "Sixers",
            "Bucks",
            "Suns",
            "Nuggets",
            "Clippers",
            "Mavericks",
            "Grizzlies",
            "Pelicans",
            "Timberwolves",
            "Thunder",
            "Rockets",
            "Spurs",
            "Jazz",
            "Kings",
            "Blazers",
            "Hornets",
            "Hawks",
            "Magic",
            "Pacers",
            "Pistons",
            "Cavaliers",
            "Wizards",
            "Raptors",
        ]

        for team in nba_teams:
            if team.lower() in text.lower():
                entities["team"] = team
                break

        player_pattern = (
            r"(?:LeBron|Stephen Curry|Kevin Durant|Giannis|Luka|Jayson Tatum)"
        )
        player_match = re.search(player_pattern, text, re.IGNORECASE)
        if player_match:
            entities["player"] = player_match.group()

        year_pattern = r"\b(20\d{2}|19\d{2})\b"
        year_match = re.search(year_pattern, text)
        if year_match:
            entities["year"] = year_match.group()

        season_pattern = r"\b(20\d{2}-\d{2})\b"
        season_match = re.search(season_pattern, text)
        if season_match:
            entities["season"] = season_match.group()

        return entities

    def _build_comparison_context(self, turn: ConversationTurn) -> str:
        """Build context for comparison queries like "how about X?".

        Args:
            turn: The previous turn to build context from.

        Returns:
            Context string to substitute.
        """
        if turn.rewritten_query:
            parts = turn.rewritten_query.split()
            action_words = ["show", "get", "find", "list", "compare", "what"]
            for i, word in enumerate(parts):
                if word.lower() in action_words:
                    return " ".join(parts[i:])

        return ""

    def clear(self) -> None:
        """Clear all conversation history."""
        with self._lock:
            self._turns.clear()
            self._last_tables = []
            self._last_sql = None

    def get_history_summary(self) -> str:
        """Get a brief summary of conversation history.

        Returns:
            Human-readable summary of recent conversations.
        """
        with self._lock:
            if not self._turns:
                return "No conversation history."

            lines = []
            for i, turn in enumerate(self._turns, 1):
                q = (
                    turn.question[:50] + "..."
                    if len(turn.question) > 50
                    else turn.question
                )
                lines.append(f"{i}. Q: {q}")

            return "\n".join(lines)


class MemoryManager:
    """Manage per-user conversation memory instances."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._memories: dict[str, ConversationMemory] = {}

    def get_memory(self, user_id: str | None) -> ConversationMemory:
        key = user_id or "default"
        with self._lock:
            memory = self._memories.get(key)
            if memory is None:
                memory = ConversationMemory()
                self._memories[key] = memory
            return memory


_MEMORY_MANAGER = MemoryManager()


def get_memory(user_id: str | None = None) -> ConversationMemory:
    """Get a conversation memory instance for a user."""
    return _MEMORY_MANAGER.get_memory(user_id)


def add_turn(
    question: str,
    answer: str,
    sql: str | None = None,
    *,
    user_id: str | None = None,
) -> None:
    """Convenience function to add a turn to memory.

    Args:
        question: The user's question.
        answer: The system's answer.
        sql: The SQL query used.
    """
    get_memory(user_id).add_turn(question, answer, sql)


def get_context(n_turns: int = 3, *, user_id: str | None = None) -> ConversationContext:
    """Convenience function to get conversation context.

    Args:
        n_turns: Number of turns to retrieve.

    Returns:
        Conversation context.
    """
    return get_memory(user_id).get_context(n_turns)


def extract_references(
    query: str, *, user_id: str | None = None
) -> ResolvedReferences:
    """Convenience function to resolve references.

    Args:
        query: Query with potential references.

    Returns:
        Resolved references.
    """
    return get_memory(user_id).extract_references(query)
