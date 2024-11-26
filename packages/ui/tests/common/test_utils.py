from ui.common.utils import (
    as_labels,
    id,
    snake_to_title,
)


def test_id():
    """Test the id function."""
    assert id(page="page", section="section", component="component", element="element") == {
        "page": "page",
        "section": "section",
        "component": "component",
        "element": "element",
    }
    assert id(page="page", section="section", component="component") == {
        "page": "page",
        "section": "section",
        "component": "component",
    }
    assert id(page="page", section="section") == {"page": "page", "section": "section"}

    assert id(page="map", section="map", component="map") == {"page": "map", "section": "map", "component": "map"}
    assert id(page="map", section="map", component="map", element="tooltip") == {
        "page": "map",
        "section": "map",
        "component": "map",
        "element": "tooltip",
    }


def test_list_to_labels():
    """Test the list_to_labels function."""
    assert as_labels(["a", "b", "c"]) == [
        {"label": "A", "value": "a"},
        {"label": "B", "value": "b"},
        {"label": "C", "value": "c"},
    ]
    assert as_labels(["a_b", "a_c", "b_c"]) == [
        {"label": "A B", "value": "a_b"},
        {"label": "A C", "value": "a_c"},
        {"label": "B C", "value": "b_c"},
    ]
    assert as_labels(["SA1_2021", "SA2_2021", "SA3_2021"]) == [
        {"label": "SA1 2021", "value": "SA1_2021"},
        {"label": "SA2 2021", "value": "SA2_2021"},
        {"label": "SA3 2021", "value": "SA3_2021"},
    ]


def test_snake_to_title():
    """Test the snake_to_title function."""
    assert snake_to_title("a_b_c") == "A B C"
    assert snake_to_title("a b c") == "A B C"
    assert snake_to_title("hello_world") == "Hello World"
    assert snake_to_title("SA1_2021") == "SA1 2021"
