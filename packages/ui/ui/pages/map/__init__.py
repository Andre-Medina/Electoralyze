import dash_leaflet as dl

# import dash_mantine_components as dmc
from dash import Input, Output, clientside_callback, html

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

INITIAL_COORDS = (-27.5, 134)
INITIAL_ZOOM = 5


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
        colour_bar = id(page="map", section="map", component="colour_bar")
        highlight_layer = id(page="map", section="map", component="highlight_layer")
        geo_json_layer = id(page="map", section="map", component="geo_json_layer")

    def __init__(self):
        """Page for maps."""
        tile_layer = dl.TileLayer(
            url=MAP_TILES_LIGHT,
            attribution='&copy; <a href="https://stadiamaps.com/">Stadia Maps</a> ',
            id=self.ids.tile_layer,
        )

        geo_json_layer = dl.GeoJSON(
            children=dl.Tooltip(id=self.ids.tooltip),
            id=self.ids.geo_json_layer,
        )

        highlight_layer = dl.Pane(
            dl.GeoJSON(
                id=self.ids.highlight_layer,
                style={"weight": 3, "color": "black", "fillOpacity": 0},
                interactive=False,
            ),
            name="highlight",
            style={"pointerEvents": "none"},
        )

        colour_bar = html.Div(id=self.ids.colour_bar, style={"position": "absolute"})

        layout = html.Div(
            dl.Map(
                children=[
                    tile_layer,
                    geo_json_layer,
                    highlight_layer,
                    colour_bar,
                ],
                center=INITIAL_COORDS,
                zoom=INITIAL_ZOOM,
                zoomControl=False,
                style={
                    "height": "calc(100vh - var(--topbar-height))",
                    "maxHeight": "100%",
                    "zIndex": 0,
                },
            ),
            style={"flex": 1},
        )

        super().__init__(layout)


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
