import dash_leaflet as dl
import dash_mantine_components as dmc
import geojson
import requests
from dash import Input, Output, callback, clientside_callback, html
from dash_extensions.javascript import arrow_function

from ui.common import Page, Scaffold, icon, id

# Had API key in URL but works fine without?
MAP_TILES_LIGHT = "https://tile.jawg.io/jawg-light/{z}/{x}/{y}{r}.png"
MAP_TILES_DARK = "https://tile.jawg.io/jawg-dark/{z}/{x}/{y}{r}.png"
MAP_DISTRIBUTION = '&copy; <a href="https://www.jawg.io/en/">Jawg Maps</a> '
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
        geojson_layer = id(page="map", section="map", component="geojson_layer")

        page_loaded = id(page="map", section="loader", component="button")
        loader = id(page="map", section="loader", component="loader")

    def __init__(self):
        """Page for maps."""
        tile_layer = dl.TileLayer(
            url=MAP_TILES_LIGHT,
            attribution=MAP_DISTRIBUTION,
            id=self.ids.tile_layer,
        )

        geojson_layer = dl.GeoJSON(
            children=dl.Tooltip(id=self.ids.tooltip),
            id=self.ids.geojson_layer,
            hoverStyle=arrow_function({"weight": 5, "color": "#666", "dashArray": ""}),
            style={"weight": 5, "color": "purple", "dashArray": ""},
        )

        colour_bar = html.Div(id=self.ids.colour_bar, style={"position": "absolute"})

        layout = [
            html.Div(
                dl.Map(
                    children=[
                        tile_layer,
                        geojson_layer,
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
            ),
            html.Div(dmc.Loader(), id=self.ids.loader, className="map-loader"),
            dmc.Button(id=self.ids.page_loaded, style={"display": "none"}),
        ]

        super().__init__(layout)


# TODO: Add fly to regions
# @app.callback(Output("map", "viewport"), Input("btn", "n_clicks"), prevent_initial_call=True)
# def fly_to_paris(_):
#     return dict(center=[48.864716, 2.349014], zoom=10, transition="flyTo")


clientside_callback(
    f"(isLightMode) => isLightMode ? '{MAP_TILES_LIGHT}' : '{MAP_TILES_DARK}'",
    Output(Map.ids.tile_layer, "url"),
    Input(Scaffold.ids.dark_mode_toggle, "checked"),
)


def fetch_geojson(url):
    """
    Fetches a GeoJSON file from the web and parses it.

    :param url: URL of the GeoJSON file
    :return: Parsed GeoJSON data
    """
    try:
        response = requests.get(url)  # noqa S113
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = geojson.loads(response.text)
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching GeoJSON: {e}")
        return None
    except geojson.GeoJSONDecodeError as e:
        print(f"Error decoding GeoJSON: {e}")
        return None


@callback(
    Output(Map.ids.geojson_layer, "data"),
    Input(Map.ids.page_loaded, "n_clicks"),
    running=[(Output(Map.ids.loader, "className"), "map-loader visible", "map-loader")],
)
def update_map_data(*_) -> dict:
    """Update map data when page loads."""
    import time

    time.sleep(1)  # simulate loading
    geojson_url = "https://raw.githubusercontent.com/datasets/geo-boundaries-world-110m/master/countries.geojson"
    geojson_data = fetch_geojson(geojson_url)

    for feature in geojson_data["features"]:
        if "properties" in feature and "name" in feature["properties"]:
            feature["name"] = feature["properties"]["name"]

    return geojson_data


clientside_callback(
    """(hoverData) =>  hoverData == null ? null: hoverData.name;""",
    Output(Map.ids.tooltip, "children"),
    Input(Map.ids.geojson_layer, "hoverData"),
)
