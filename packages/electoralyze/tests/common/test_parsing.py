import polars as pl
import pytest
from electoralyze.common.parsing import clean_string


@pytest.mark.parametrize(
    "input_text,expected",
    [
        # Basic cases
        ("Hello World", "hello_world"),
        ("Hello World Space", "hello_world_space"),
        ("Test  String", "test_string"),
        ("sample-text", "sample_text"),
        ("UPPERCASE", "uppercase"),
        # Special characters
        ("text.with.dots", "text_with_dots"),
        ("text - with - dashes", "text_with_dashes"),
        ("text'with'quotes", "text_with_quotes"),
        ('text"with"quotes', "text_with_quotes"),
        # Whitespace handling
        ("  leading space", "leading_space"),
        ("trailing space  ", "trailing_space"),
        ("  multiple  spaces  ", "multiple_spaces"),
        (" - dash with spaces - ", "_dash_with_spaces_"),
        # Empty strings
        ("", ""),
        (" ", ""),
        ("  ", ""),
        # Mixed cases
        ("Mixed.CASE-example", "mixed_case_example"),
        ("Another - MIXED.case", "another_mixed_case"),
        ("UPPER.lower.MIXED", "upper_lower_mixed"),
        ("UPPER.lower. MIXED 'more", "upper_lower_mixed_more"),
    ],
)
def test_clean_string(input_text, expected):
    """Test the clean_string function."""
    df = pl.DataFrame({"text": [input_text]})
    result = df.with_columns(cleaned=clean_string("text"))
    assert result["cleaned"][0] == expected
