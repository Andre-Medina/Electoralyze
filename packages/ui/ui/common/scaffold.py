import dash
import dash_mantine_components as dmc
from dash import Input, Output, clientside_callback, html
from dash_iconify import DashIconify

from ui.common.id import id
from ui.common.page import Page

# Set new react version.
dash._dash_renderer._set_react_version("18.2.0")


class Scaffold(dmc.MantineProvider):
    """UI Scaffold, refer to init."""

    class ids:
        """Ids for the scaffold."""

        theme_toggle = id(page="scaffold", section="nav_bar", component="theme_toggle")
        scaffold = id(page="scaffold", section="scaffold")

    def __init__(self, name: str, pages: list[Page.__class__]):
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
                create_header(name=name, pages=pages),
                html.Hr(),
                dash.page_container,
            ]
        )
        # layout = [dmc.Button("Custom Colors!", color="myColor")]
        super().__init__(
            id=self.ids.scaffold,
            children=layout,
            theme=theme,
        )


def create_header(name: str, pages: list[Page.__class__]) -> html.Header:
    """Creates the header bar for the page."""
    logo = html.H1(html.A(name, href="/", style={"text-decoration": "none"}))
    links = dmc.Group([html.A(dmc.Button(children=page.label), href=page.path) for page in pages])
    theme_toggle = make_theme_toggle()

    header = html.Header(
        [logo, links, theme_toggle],
        style={
            "height": "var(--topbar-height)",
            "flex": "0 0 auto",
            "padding": "0 1rem",
            "display": "flex",
            "alignItems": "center",
            "justifyContent": "space-between",
        },
    )

    return header


def make_theme_toggle():
    """Theme switch."""
    return dmc.Switch(
        offLabel=DashIconify(icon="radix-icons:moon", height=18),
        onLabel=DashIconify(icon="radix-icons:sun", height=18),
        size="lg",
        persistence=True,
        checked=True,
        id=Scaffold.ids.theme_toggle,
    )


clientside_callback(
    """(isLightMode) => isLightMode ? 'light' : 'dark'""",
    Output(Scaffold.ids.scaffold, "forceColorScheme"),
    Input(Scaffold.ids.theme_toggle, "checked"),
    prevent_initial_callback=True,
)
