import dash_mantine_components as dmc
from dash import Dash, register_page

from ui.common.page import Page
from ui.common.scaffold import Scaffold


def create_app(
    server_name: str,
    site_name: str,
    site_colour: str,
    pages: list[Page.__class__],
) -> Dash:
    """Create the application as a whole.

    - Initializes Dash
    - Creates scaffold
    - Attaches pages

    Parameters
    ----------
    server_name : str, Name of the server to pass to Dash, `__name__` typically.
    site_name : str, Human readable name of the site.
    site_colour: str, primary colour for the site.
    pages : list[Page.__class__], List of pages to be rendered.

    Returns
    -------
    Dash, dash app with scaffold and pages ready to go.
    """
    app = Dash(
        server_name,
        use_pages=True,
        compress=True,
        suppress_callback_exceptions=True,
        pages_folder="",
        assets_url_path="/assets",
        external_stylesheets=dmc.styles.ALL,  # type: ignore
    )

    app.layout = Scaffold(
        name=site_name,
        pages=pages,
        site_colour=site_colour,
    )

    for page in pages:
        register_page(page.label, path=page.path, layout=page)

    return app
