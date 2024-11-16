import dash_mantine_components as dmc
from dash import Input, Output, callback, dcc, html

from ui.common import Page


class Analytics(Page):
    """Page for Analytics."""

    path = "/analytics"
    label = "Analytics"

    def __init__(self):
        """Page for Analytics."""
        layout = dmc.Stack(
            [
                html.H1("This is our Analytics page"),
                html.Div(
                    [
                        "Select a city: ",
                        dcc.RadioItems(
                            options=["New York City", "Montreal", "San Francisco"],
                            value="Montreal",
                            id="analytics-input",
                        ),
                    ]
                ),
                html.Br(),
                html.Div(id="analytics-output"),
            ]
        )

        super().__init__(layout)


@callback(Output("analytics-output", "children"), Input("analytics-input", "value"))
def update_city_selected(input_value):
    """Callback for updating cities."""
    return f"You selected: {input_value}"
