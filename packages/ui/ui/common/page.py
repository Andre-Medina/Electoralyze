import dash_mantine_components as dmc

NEEDED_PAGE_ATTRIBUTES = ["path", "label"]


class Page(dmc.Stack):
    """Page base class, refer to __init__."""

    path: str
    label: str
    icon: str | None = None

    def __init__(self, children, *args, **kwargs):
        """Base Page class to organise pages with.

        Inherit and set all NEEDED_PAGE_ATTRIBUTES
        """
        if any((getattr(self, attribute) is None) for attribute in NEEDED_PAGE_ATTRIBUTES):
            raise RuntimeError(f"Please set all page attributes: {NEEDED_PAGE_ATTRIBUTES}")
        children_wrapped = dmc.Container(children)

        super().__init__(*args, children=children_wrapped, **kwargs)
