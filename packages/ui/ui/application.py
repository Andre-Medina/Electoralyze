import dash
from dash import Dash, html, dcc, register_page

from ui.common.make_app import create_application

from ui.pages import Analytics, Home, Archive


app = create_application(
    name = "Electoralyze",
    pages = [Analytics, Home, Archive],
    # page_base= ,
)

# Expose server for Flask
server = app.server 

if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_ui=True, host="localhost")