import dash
from dash import html
from ui.common.page_base import Page

# dash.register_page(__name__, path='/')



class Home(Page):
    
    path = "/"
    label = "Home"

    def __init__(self):

        layout = html.Div([
            html.H1('This is our Home page'),
            html.Div('This is our Home page content.'),
        ])

        super().__init__(layout)