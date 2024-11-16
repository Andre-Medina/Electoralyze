from ui.common import UIScaffold, create_application
from ui.pages import Analytics, Archive, Home

app = create_application(
    name="Electoralyze",
    pages=[Analytics, Home, Archive],
    page_base=UIScaffold,
)

# Expose server for Flask
server = app.server

if __name__ == "__main__":
    app.run_server(debug=True, dev_tools_ui=True, host="localhost")
