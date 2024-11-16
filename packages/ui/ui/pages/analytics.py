import dash
from dash import html, dcc, callback, Input, Output

from ui.common.page_base import Page

class Analytics(Page):
    
    path = "/analytics"
    label = "Analytics"

    def __init__(self):

        layout = html.Div([
            html.H1('This is our Analytics page'),
            html.Div([
                "Select a city: ",
                dcc.RadioItems(
                    options=['New York City', 'Montreal', 'San Francisco'],
                    value='Montreal',
                    id='analytics-input'
                )
            ]),
            html.Br(),
            html.Div(id='analytics-output'),
        ])


        super().__init__(layout)


@callback(
    Output('analytics-output', 'children'),
    Input('analytics-input', 'value')
)
def update_city_selected(input_value):
    return f'You selected: {input_value}'