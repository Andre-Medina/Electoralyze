from dash import html

from ui.common import Page, icon

# dash.register_page(__name__, path='/')


class Home(Page):
    """Home page."""

    path = "/"
    label = "Home"
    icon = icon.home

    def __init__(self):
        """Home page."""
        layout = html.Div(
            [
                html.H1("This is our Home page"),
                html.Div("This is our Home page content."),
            ]
        )

        super().__init__(layout)
