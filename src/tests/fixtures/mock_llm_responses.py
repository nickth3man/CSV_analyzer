"""Shared mock responses for LLM calls in tests."""

from __future__ import annotations


def mock_llm_response(prompt: str) -> str:
    """Generate deterministic mock responses based on prompt content."""
    prompt_lower = prompt.lower()

    if "analyzing if a user's nba question" in prompt_lower:
        return """```yaml
intent: clear
reasoning: "Query is specific enough"
clarification_questions: []
```"""

    if "rewriting a user's nba question" in prompt_lower:
        return """```yaml
rewritten_query: "Who led the league in points in 2023?"
resolved_entities: {}
reasoning: "No changes needed"
```"""

    if "determine if the question is simple" in prompt_lower:
        return """```yaml
complexity: simple
combination_strategy: synthesize
sub_queries: []
```"""

    if "select the most relevant tables" in prompt_lower:
        if "rolling" in prompt_lower or "trend" in prompt_lower:
            return """```yaml
selected_tables:
  - table_name: team_rolling_metrics
    reason: Contains rolling averages for team performance.
  - table_name: team_gold
    reason: Canonical team information.
```"""
        if "season" in prompt_lower and "average" in prompt_lower:
            return """```yaml
selected_tables:
  - table_name: player_season_averages
    reason: Contains pre-calculated season averages for players.
  - table_name: player_gold
    reason: Canonical player information.
```"""
        if "standing" in prompt_lower or "rank" in prompt_lower:
            return """```yaml
selected_tables:
  - table_name: team_standings
    reason: Contains current league standings and win percentages.
```"""
        return """```yaml
selected_tables:
  - table_name: player
    reason: "Contains player information"
  - table_name: game
    reason: "Contains game stats"
```"""

    if "duckdb sql expert" in prompt_lower:
        return """```yaml
thinking: |
  Use a simple aggregation on the game table.
sql: |
  SELECT player_name, SUM(points) AS total_points
  FROM player_game_stats
  WHERE season = '2022-23'
  GROUP BY player_name
  ORDER BY total_points DESC
  LIMIT 1;
```"""

    if "generate a duckdb sql query" in prompt_lower:
        if "team_rolling_metrics" in prompt_lower:
            return """```sql
SELECT team_name, rolling_win_pct
FROM team_rolling_metrics
WHERE rolling_window = 10
ORDER BY rolling_win_pct DESC
LIMIT 5;
```"""
        if "player_season_averages" in prompt_lower:
            return """```sql
SELECT player_name, pts_avg
FROM player_season_averages
WHERE season_id = '2023-24'
ORDER BY pts_avg DESC
LIMIT 5;
```"""
        if "team_standings" in prompt_lower:
            return """```sql
SELECT team_name, wins, losses, win_pct
FROM team_standings
ORDER BY win_pct DESC;
```"""
        return "```sql\nSELECT * FROM player_gold LIMIT 5;\n```"

    if "quality assurance reviewer" in prompt_lower:
        return """```yaml
status: pass
confidence: 0.9
issues: []
suggestions: []
```"""

    if "nba data analyst explaining query results" in prompt_lower:
        return """```yaml
answer: |
  The top scorer in 2023 was Player X with 2,000 points.
transparency_note: |
  I summed points by player for the 2022-23 season and sorted the totals.
```"""

    if "extract all named entities" in prompt_lower:
        return '["LeBron James", "Tracy McGrady"]'

    if "create a comprehensive analysis plan" in prompt_lower:
        return """1. Query the 'stats' table for LeBron James and Tracy McGrady.
2. Filter for relevant years.
3. Compare points.
4. Generate insights."""

    if "write comprehensive code" in prompt_lower or "fix it" in prompt_lower:
        return """
final_result = {}
try:
    df = dfs['stats']
    lebron = df[df['player_name'] == 'LeBron James']
    tracy = df[df['player_name'] == 'Tracy McGrady']
    final_result['LeBron James'] = lebron.to_dict('records')
    final_result['Tracy McGrady'] = tracy.to_dict('records')
except Exception as e:
    final_result['error'] = str(e)
"""

    if "analyze the following data" in prompt_lower:
        return """
```json
{
"key_stats": {"LeBron Points": 30000, "Tracy Points": 18000},
"comparison": "LeBron has more points.",
"insights": ["LeBron played longer."],
"data_gaps": [],
"narrative_points": ["LeBron is great", "Tracy was good"]
}
```"""

    if "writing a response to a user's question" in prompt_lower:
        return "LeBron James scored 30000 points and Tracy McGrady scored 18000 points. LeBron had a longer career."

    return "Mock LLM response for testing purposes."
