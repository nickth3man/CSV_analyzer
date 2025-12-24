import os


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
