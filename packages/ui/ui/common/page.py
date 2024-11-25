import dash_mantine_components as dmc

NEEDED_PAGE_ATTRIBUTES = ["path", "label"]


class Page(dmc.Stack):
    """Page base class for creating pages.

    To create a page you must follow the following steps
    - Create a file for you page In `ui/pages`
    - Inherit this class and
      -

    Example
    -------

    """

    path: str
    label: str
    icon: str | None = None
    wrap_with_container: bool = True

    def __init__(self, children, *args, **kwargs):
        """Base Page class to organise pages with.

        Inherit and set all NEEDED_PAGE_ATTRIBUTES
        """
        if any((getattr(self, attribute) is None) for attribute in NEEDED_PAGE_ATTRIBUTES):
            raise RuntimeError(f"Please set all page attributes: {NEEDED_PAGE_ATTRIBUTES}")

        if self.wrap_with_container:
            children = dmc.Container(children)

        super().__init__(
            *args,
            children=children,
            **kwargs,
        )
