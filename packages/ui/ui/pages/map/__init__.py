from enum import Enum

import dash_leaflet as dl
import dash_mantine_components as dmc
import geojson
import requests
from dash import ClientsideFunction, Input, Output, callback, clientside_callback, html
from dash_extensions.javascript import arrow_function
from dash_iconify import DashIconify
from electoralyze import region
from electoralyze.common.geometry import to_geopandas

from ui.common import Page, Scaffold, icon, id
from ui.common.utils import as_labels

# Goto but wants API key
# MAP_TILES_LIGHT = "https://{s}.tile.jawg.io/jawg-light/{z}/{x}/{y}{r}.png"
# MAP_TILES_DARK = "https://{s}.tile.jawg.io/jawg-dark/{z}/{x}/{y}{r}.png"
# MAP_DISTRIBUTION = '&copy; <a href="https://www.jawg.io/en/">Jawg Maps</a> '
# 'https://tile.jawg.io/jawg-matrix/{z}/{x}/{y}{r}.png',

# These looked good, but slow to load without API key
# MAP_TILES_DARK = 'https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png',
# MAP_TILES_LIGHT = 'https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png',
# MAP_DISTRIBUTION = '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a> '

# Other options
MAP_TILES_LIGHT = "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
MAP_TILES_DARK = "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
MAP_DISTRIBUTION = '&copy; <a href="https://carto.com/basemaps">Carto Maps</a> '

# MAP_TILES_LIGHT="https://tile.openstreetmap.org/{z}/{x}/{y}.png",
# Find more maps here: https://leaflet-extras.github.io/leaflet-providers/preview/

INITIAL_COORDS = (-27.5, 134)
INITIAL_ZOOM = 5


class ZIndex(int, Enum):
    """Enum for ZIndexs."""

    BASE_MAP = 0
    CONTROL_LEGEND = 1
    CONTROL_DROPDOWNS = 2


class DataExtra(str, Enum):
    """Enum for dummy data options."""

    EXTRA = "extra"
    MORE = "more"


class DataSource(str, Enum):
    """Enum for data source."""

    COUNTRY = "countries"
    STATE = "states"


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

        data_source = id(page="map", section="controls", component="data_source")
        data_extra = id(page="map", section="controls", component="data_extra")

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

        map_layout = html.Div(
            dl.Map(
                children=[
                    tile_layer,
                    geojson_layer,
                    colour_bar,
                ],
                id=self.ids.map,
                center=INITIAL_COORDS,
                zoom=INITIAL_ZOOM,
                zoomControl=False,
                style={
                    "height": "calc(100vh - var(--topbar-height))",
                    "maxHeight": "100%",
                    "zIndex": ZIndex.BASE_MAP,
                },
            ),
            style={"flex": 1},
        )

        controls = [
            dmc.Select(
                id=self.ids.data_source,
                label="Data displayed",
                data=as_labels(DataSource) + as_labels(region.ALL_IDS),
                persistence=True,
                value=DataSource.COUNTRY,
                leftSection=DashIconify(icon=icon.analytics, height=19),
                comboboxProps={"position": "bottom", "zIndex": ZIndex.CONTROL_DROPDOWNS},
                allowDeselect=False,
            ),
            dmc.Select(
                id=self.ids.data_extra,
                allowDeselect=False,
                label="Other option",
                data=as_labels(DataExtra),
                persistence=True,
                value=DataExtra.MORE,
                comboboxProps={"position": "bottom", "zIndex": ZIndex.CONTROL_DROPDOWNS},
            ),
        ]

        controls_legend = dmc.Accordion(
            value="legend",
            disableChevronRotation=True,
            styles={
                "panel": {"display": "flex", "flexDirection": "column", "gap": "1rem", "zIndex": ZIndex.CONTROL_LEGEND}
            },
            style={"zIndex": ZIndex.CONTROL_LEGEND},
            className="map-legend",
            children=dmc.AccordionItem(
                style={"zIndex": ZIndex.CONTROL_LEGEND},
                value="legend",
                children=[
                    dmc.AccordionControl("", chevron=DashIconify(icon="gg:menu-right", height=20)),
                    dmc.AccordionPanel(controls),
                ],
            ),
        )

        layout = [
            map_layout,
            controls_legend,
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
    Input(Map.ids.data_source, "value"),
    Input(Map.ids.data_extra, "value"),
    running=[(Output(Map.ids.loader, "className"), "map-loader visible", "map-loader")],
)
def update_map_data(_n_clicks, data_source, data_extra) -> dict:
    """Update map data when page loads."""
    match data_source:
        case DataSource.STATE:
            geo_url = "https://raw.githubusercontent.com/rowanhogan/australian-states/refs/heads/master/states.geojson"
            name_prop = "STATE_NAME"
            geojson_data = fetch_geojson(geo_url)

        case DataSource.COUNTRY:
            geo_url = "https://raw.githubusercontent.com/datasets/geo-boundaries-world-110m/master/countries.geojson"
            name_prop = "name"
            geojson_data = fetch_geojson(geo_url)
        case _:
            geojson_data = region.from_id(data_source).geometry.pipe(to_geopandas).to_geo_dict()
            name_prop = data_source

    for feature in geojson_data["features"]:
        if "properties" in feature and name_prop in feature["properties"]:
            feature["name"] = feature["properties"][name_prop]
            feature["data_extra"] = data_extra

    return geojson_data


clientside_callback(
    ClientsideFunction(namespace="ui", function_name="mapLabel"),
    Output(Map.ids.tooltip, "children"),
    Input(Map.ids.geojson_layer, "hoverData"),
)
