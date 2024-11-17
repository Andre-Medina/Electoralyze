import dash
import dash_mantine_components as dmc
from dash import Input, Output, clientside_callback, dcc, html
from dash.dash import (
    _ID_CONTENT,
    _ID_DUMMY,
    _ID_LOCATION,
    _ID_STORE,
)
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
        loading_overlay = id(page="scaffold", section="loading_overlay")

        location = _ID_LOCATION
        content = _ID_CONTENT
        store = _ID_STORE
        dummy = _ID_DUMMY

    def __init__(self, name: str, pages: list[Page.__class__]):
        """Scaffold for the UI, takes page list and generates the nav bar and base page."""
        layout = html.Div(
            [
                create_header(name=name, pages=pages),
                html.Hr(),
                create_body(),
            ]
        )

        super().__init__(
            id=self.ids.scaffold,
            children=layout,
        )


def create_header(name: str, pages: list[Page.__class__]) -> html.Header:
    """Creates the header bar for the page."""
    logo = html.H1(html.A(name, href="/", style={"text-decoration": "none"}))
    links = dmc.Group([html.A(dmc.Button(children=page.label), href=page.path) for page in pages])
    theme_toggle = create_dark_mode_toggle()

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


def create_dark_mode_toggle():
    """Theme switch."""
    return dmc.Switch(
        # offLabel=DashIconify(icon="iconoir:moon-sat", height=18),
        offLabel=DashIconify(icon="line-md:moon-alt-loop", height=18),
        # offLabel=DashIconify(icon="line-md:moon-simple", height=18),
        # onLabel=DashIconify(icon="line-md:sunny-outline", height=18),
        onLabel=DashIconify(icon="line-md:sunny-loop", height=18),
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


def create_body() -> dmc.ScrollArea:
    """Creates the loading body and space for data."""
    body_contents = html.Div(
        [
            dmc.LoadingOverlay(
                visible=False,
                id=Scaffold.ids.loading_overlay,
                overlayProps={"radius": "sm", "blur": 2},
                zIndex=10,
            ),
            dcc.Location(id=Scaffold.ids.location, refresh="callback-nav"),
            html.Div(id=Scaffold.ids.content, disable_n_clicks=True),
            dcc.Store(id=Scaffold.ids.store),
            html.Div(id=Scaffold.ids.dummy, disable_n_clicks=True),
        ]
    )

    body = dmc.ScrollArea(
        offsetScrollbars=False,
        type="auto",
        children=[body_contents],
        id="content-scroll",
    )
    return body
