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

from ui.common import icon
from ui.common.id import id
from ui.common.page import Page

# Set new react version.
dash._dash_renderer._set_react_version("18.2.0")  # type: ignore


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

    def __init__(self, name: str, site_colour: str, pages: list[Page.__class__]):
        """Scaffold for the UI.

        Creates the basic layout of every page including
        - the nav bar (Will auto generate the nav bar from given pages.)
        - theme toggle and
        - container for page contents.

        Parameters
        ----------
        name: str, Name of the website.
        pages: list[Page], list of page classes with navigation info in them.
        site_colour: str, primary colour for the site.
        """
        layout = html.Div(
            [
                create_header(name=name, pages=pages, site_colour=site_colour),
                html.Hr(),
                create_body(),
            ]
        )

        theme = create_theme(site_colour=site_colour)

        super().__init__(
            id=self.ids.scaffold,
            children=layout,
            theme=theme,
        )


def create_header(name: str, site_colour: str, pages: list[Page.__class__]) -> html.Header:
    """Creates the header bar for the Scaffold.

    Parameters
    ----------
    name: str, name of the site to put in the top right corner.
    pages: list[Page], list of pages to render as navigation buttons.
    site_colour: str, primary colour for the site.
    """
    logo = html.H3(html.A(name, href="/", style={"text-decoration": "none", "color": site_colour}))

    links = dmc.Group(
        [
            html.A(
                dmc.Button(
                    children=page.label,
                    leftSection=None
                    if page.icon is None
                    else html.Div(DashIconify(icon=page.icon, height=16, width=16), style={"width": "16px"}),
                ),
                href=page.path,
            )
            for page in pages
        ]
    )

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
    """Create a `dmc.Switch` component used to toggle between dark and light mode."""
    return dmc.Switch(
        offLabel=DashIconify(icon=icon.moon, height=18),
        onLabel=DashIconify(icon=icon.sun, height=18),
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
    """Creates the body for the scaffold.

    body includes
    - Loading Overly
    - Content div
    - everything else in dash.page_container
    """
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


def create_theme(site_colour: str) -> dict:
    """Create theme as a dict, attributes will override existing `dmc.DEFAULT_THEME`."""
    # dmc.DEFAULT_THEME

    theme = {"primaryColor": site_colour}

    return theme
