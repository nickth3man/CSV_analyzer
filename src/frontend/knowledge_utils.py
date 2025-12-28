"""Utilities for managing the knowledge store."""

from backend.utils.knowledge_store import knowledge_store


def get_knowledge_store_data():
    """Produce a human-readable, formatted summary of the current knowledge store.

    The resulting text contains three sections:
    - Entity Mappings: each entity with its tables and columns.
    - Successful Query Patterns: each query type with the number of learned patterns.
    - Join Patterns: up to the first five join patterns with tables and keys.

    Returns:
        str: The assembled formatted string.
    """
    data = knowledge_store.data

    output = "## Entity Mappings\n"
    if data.get("entity_mappings"):
        for entity, tables in data["entity_mappings"].items():
            output += f"**{entity}**\n"
            for table, cols in tables.items():
                output += f"  - {table}: {', '.join(cols)}\n"
    else:
        output += "No entity mappings yet.\n"

    output += "\n## Successful Query Patterns\n"
    if data.get("successful_patterns"):
        for qtype, patterns in data["successful_patterns"].items():
            output += f"**{qtype}**: {len(patterns)} patterns\n"
    else:
        output += "No patterns learned yet.\n"

    output += "\n## Join Patterns\n"
    if data.get("join_patterns"):
        for pattern in data["join_patterns"][:5]:
            output += f"- Tables: {pattern['tables']}, Keys: {pattern['keys']}\n"
    else:
        output += "No join patterns discovered yet.\n"

    return output


def clear_knowledge_store() -> str:
    """Clear all data from the knowledge store."""
    knowledge_store.data = {
        "entity_mappings": {},
        "successful_patterns": {},
        "column_hints": {},
        "join_patterns": [],
    }
    knowledge_store.save()
    return "Knowledge store cleared!"
