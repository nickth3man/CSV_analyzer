import os
from pathlib import Path


def sanitize_csv_filename(filename: str) -> str | None:
    if not filename:
        return None

    normalized = filename.replace("\\", "/")
    basename = os.path.basename(normalized)

    if "\x00" in basename:
        basename = basename.split("\x00")[0]

    if not basename.lower().endswith(".csv"):
        basename += ".csv"

    if not basename or basename.startswith("."):
        return None

    if "/" in basename or "\\" in basename:
        return None

    return basename


def resolve_safe_dir(
    candidate: str | None,
    *,
    base_dir: Path,
    default: str | Path,
) -> str:
    """Resolve a directory path within a base directory to prevent traversal."""
    base_dir = base_dir.resolve()
    default_path = Path(default)
    if not default_path.is_absolute():
        default_path = (base_dir / default_path).resolve()

    if not candidate:
        return str(default_path)

    candidate_path = Path(candidate)
    if not candidate_path.is_absolute():
        candidate_path = base_dir / candidate_path

    resolved = candidate_path.resolve()
    if resolved != base_dir and base_dir not in resolved.parents:
        return str(default_path)

    return str(resolved)
