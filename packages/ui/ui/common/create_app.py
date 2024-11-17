import dash_mantine_components as dmc
from dash import Dash, register_page

from ui.common.page import Page
from ui.common.scaffold import Scaffold


def create_app(
    name: str,
    pages: list[Page.__class__],
) -> Dash:
    """Create the application."""
    app = Dash(
        name,
        use_pages=True,
        pages_folder="",
        compress=True,
        suppress_callback_exceptions=True,
        external_stylesheets=dmc.styles.ALL,
        assets_url_path="/assets",
        # routing_callback_inputs={"state": State(ids.state, "data")},
    )

    app.layout = Scaffold(name="Electoralyze", pages=pages)

    for page in pages:
        register_page(page.label, path=page.path, layout=page)

    return app
