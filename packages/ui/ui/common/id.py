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
