import dash
import dash_mantine_components as dmc
from dash import dcc, html

from ui.common.page import Page

dash._dash_renderer._set_react_version("18.2.0")


class UIScaffold(dmc.MantineProvider):
    """UI Scaffold, refer to init."""

    def __init__(self, pages: list[Page.__class__]):
        """Scaffold for the UI, takes page list and generates the nav bar and base page."""
        theme = {
            "colors": {
                "myColor": [
                    "#F2FFB6",
                    "#DCF97E",
                    "#C3E35B",
                    "#AAC944",
                    "#98BC20",
                    "#86AC09",
                    "#78A000",
                    "#668B00",
                    "#547200",
                    "#455D00",
                ]
            },
        }
        layout = html.Div(
            [
                html.H1("Multi-page app with Dash Pages"),
                html.Div([html.Div(dcc.Link(f"{page.label} - {page.path}", href=page.path)) for page in pages]),
                dash.page_container,
            ]
        )
        # layout = [dmc.Button("Custom Colors!", color="myColor")]
        super().__init__(children=layout, theme=theme)
