from dash import html

from ui.common import Page, icon


class Archive(Page):
    """Archive page."""

    path = "/archive"
    label = "Archive"
    icon = icon.archive

    def __init__(self):
        """Archive page."""
        layout = html.Div(
            [
                html.H1("This is our Archive page"),
                html.Div("This is our Archive page content."),
            ]
        )

        super().__init__(layout)
