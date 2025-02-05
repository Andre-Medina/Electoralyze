import polars as pl


def clean_string(column: str) -> pl.Expr:
    """Cleans a string column."""
    cleaned_expression = (
        pl.col(column)
        .str.to_lowercase()
        .str.strip_chars()
        .str.replace_all("  ", " ")
        .str.replace_all(" - ", "_")
        .str.replace_all(" ", "_")
        .str.replace_all(r"\.", "_")
        .str.replace_all(r"\-", "_")
        .str.replace_all(r"\'", "_")
        .str.replace_all(r"\"", "_")
        .str.replace_all(r"\_\_", "_")
    )
    return cleaned_expression
