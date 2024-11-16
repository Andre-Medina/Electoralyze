import dash
from dash import html

from ui.common.page_base import Page


class Archive(Page):
    
    path = "/archive"
    label = "Archive"

    def __init__(self):

        layout = html.Div([
            html.H1('This is our Archive page'),
            html.Div('This is our Archive page content.'),
        ])

        super().__init__(layout)