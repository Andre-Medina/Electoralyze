from ui.common import create_app
from ui.pages import Analytics, Archive, Home

app = create_app(
    server_name=__name__,
    site_name="Electoralyze",
    site_colour="grape",
    pages=[Analytics, Home, Archive],
)

# Expose server for Flask
server = app.server

if __name__ == "__main__":
    app.run_server(debug=True, dev_tools_ui=True, host="localhost")
