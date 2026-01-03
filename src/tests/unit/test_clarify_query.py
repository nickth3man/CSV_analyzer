"""Tests for ClarifyQuery node."""

from backend.models import QueryIntent
from backend.nodes import ClarifyQuery


def test_clarify_query_clear(mocker) -> None:
    """ClarifyQuery returns clear when LLM says clear."""
    mocker.patch(
        "src.backend.nodes.query.call_llm",
        return_value="""```yaml
intent: clear
reasoning: "Query is specific"
clarification_questions: []
```""",
    )

    node = ClarifyQuery()
    shared = {"question": "Who led the league in points in 2023?"}

    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "clear"
    assert shared["intent"] == QueryIntent.CLEAR


def test_clarify_query_ambiguous(mocker) -> None:
    """ClarifyQuery returns ambiguous and stores clarification questions."""
    mocker.patch(
        "src.backend.nodes.query.call_llm",
        return_value="""```yaml
intent: ambiguous
reasoning: "Missing metric definition"
clarification_questions:
  - "Best by which metric?"
  - "Which season?"
```""",
    )

    node = ClarifyQuery()
    shared = {"question": "Who is the best player?"}

    prep_res = node.prep(shared)
    exec_res = node.exec(prep_res)
    action = node.post(shared, prep_res, exec_res)

    assert action == "ambiguous"
    assert shared["intent"] == QueryIntent.AMBIGUOUS
    assert shared["clarification_questions"]


def test_clarify_query_fills_history() -> None:
    """ClarifyQuery populates conversation_history if missing."""
    node = ClarifyQuery()
    shared = {"question": "How about the Warriors?"}

    prep_res = node.prep(shared)

    assert "conversation_history" in shared
    assert isinstance(prep_res["conversation_history"], list)
