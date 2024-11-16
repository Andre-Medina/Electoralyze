import dash
from dash import Dash, dcc, html, register_page
from dash.development.base_component import Component

from ui.common.page_base import Page


def create_application(
    name: str,
    pages: list[Page],
    page_base: Component | None = None,  # noqa: ARG001
) -> Dash:
    """Create the application."""
    app = Dash(
        name,
        use_pages=True,
        pages_folder="",
        compress=True,
        suppress_callback_exceptions=True,
        # routing_callback_inputs={"state": State(ids.state, "data")},
    )

    app.layout = html.Div(
        [
            html.H1("Multi-page app with Dash Pages"),
            html.Div(
                [
                    html.Div(dcc.Link(f"{page['name']} - {page['path']}", href=page["relative_path"]))
                    for page in dash.page_registry.values()
                ]
            ),
            dash.page_container,
        ]
    )

    for page in pages:
        register_page(page.label, path=page.path, layout=page)

    return app
