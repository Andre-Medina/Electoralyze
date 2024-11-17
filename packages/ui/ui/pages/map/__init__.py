import dash_leaflet as dl

# import dash_mantine_components as dmc
from dash import Input, Output, clientside_callback  # , dcc, html

from ui.common import Page, Scaffold, icon, id

# Had API key in URL but works fine without?
MAP_TILES_LIGHT = "https://tile.jawg.io/jawg-light/{z}/{x}/{y}{r}.png"
MAP_TILES_DARK = "https://tile.jawg.io/jawg-dark/{z}/{x}/{y}{r}.png"
# 'https://tile.jawg.io/jawg-matrix/{z}/{x}/{y}{r}.png',

# These looked good, but slow to load without API key
# url = 'https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.{ext}',
# url = 'https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png',

# Other options
# url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
# url="https://tile.openstreetmap.org/{z}/{x}/{y}.png",


class Map(Page):
    """Page for Maps."""

    path = "/map"
    label = "Map"
    icon = icon.map
    wrap_with_container = False

    class ids:
        """Ids for the map page."""

        map = id(page="map", section="map", component="map")
        tooltip = id(page="map", section="map", component="tooltip")
        tile_layer = id(page="map", section="map", component="tile_layer")
        colorbar = id(page="map", section="map", component="colorbar")
        geojson_highlight = id(page="map", section="map", component="geojson_highlight")
        geojson_layer = id(page="map", section="map", component="geojson_layer")
        loader = id(page="map", section="loader", component="loader")
        drawer = id(page="map", section="drawer", component="drawer")

    def __init__(self):
        """Page for maps."""
        super().__init__(
            dl.Map(
                dl.TileLayer(
                    url=MAP_TILES_LIGHT,
                    attribution='&copy; <a href="https://stadiamaps.com/">Stadia Maps</a> ',
                    id=self.ids.tile_layer,
                ),
                center=[56, 10],
                zoom=6,
                zoomControl=False,
                style={
                    "height": "calc(100vh - var(--topbar-height))",
                    "maxHeight": "100%",
                    "zIndex": 0,
                },
            )
        )


# @callback(Output("analytics-output", "children"), Input("analytics-input", "value"))
# def update_city_selected(input_value):
#     """Callback for updating cities."""
#     return f"You selected: {input_value}"


# TODO: Add fly to regions
# @app.callback(Output("map", "viewport"), Input("btn", "n_clicks"), prevent_initial_call=True)
# def fly_to_paris(_):
#     return dict(center=[48.864716, 2.349014], zoom=10, transition="flyTo")


clientside_callback(
    f"(isLightMode) => isLightMode ? '{MAP_TILES_LIGHT}' : '{MAP_TILES_DARK}'",
    Output(Map.ids.tile_layer, "url"),
    Input(Scaffold.ids.dark_mode_toggle, "checked"),
)
