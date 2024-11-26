import re
from collections.abc import Iterable


def id(*, page: str, section: str, component: str | None = None, element: str | None = None) -> dict:
    """Creates an id for an element.

    Parameters
    ----------
    page : str, page for id
    section : str, section of page
    component : str | None, optional, component on page, by default None
    element : str | None, optional, element within a component, by default None

    Returns
    -------
    dict, ID for the given component
    """
    if element is not None:
        if component is None:
            raise ValueError("Please pass `component` option before `element`.")
        id_ = {
            "page": page,
            "section": section,
            "component": component,
            "element": element,
        }
    elif component is not None:
        id_ = {
            "page": page,
            "section": section,
            "component": component,
        }
    else:
        id_ = {
            "page": page,
            "section": section,
        }

    return id_


def as_labels(options: list[str] | Iterable, /) -> list[dict]:
    """Converts a list of options to labels.

    Parameters
    ----------
    options : list[str], list of options

    Returns
    -------
    list[dict], list of labels

    Example
    -------
    >>> as_labels(["a", "b", "c"])
    [{"label": "a", "value": "a"}, {"label": "b", "value": "b"}, {"label": "c", "value": "c"}]
    """
    labels = [{"label": snake_to_title(option), "value": option} for option in options]
    return labels


def snake_to_title(string_snake: str, /) -> str:
    """Converts snake case to title case.

    Example
    -------
    ```
    >>> snake_to_title("hello_world")
    "Hello World"
    ```
    """
    words = re.split(r"[ _]", string_snake)
    # Cant use .capitalize() as wont work with acronyms
    string_title = " ".join(f"{word[0].upper()}{word[1:]}" for word in words)
    return string_title
