import os

import polars as pl
import polars_st as st
from electoralyze.common.constants import ROOT_DIR
from electoralyze.common.functools import classproperty
from electoralyze.region.region_abc import RegionABC

FEDERAL_ELECTION_BOUNDARY_2021_URL = (
    "https://www.aec.gov.au/Electorates/gis/files/2021-Cwlth_electoral_boundaries_ESRI.zip"
)
FEDERAL_ELECTION_BOUNDARY_2021_RAW_FILE = os.path.join(ROOT_DIR, "data/raw/2021-Cwlth_electoral_boundaries_ESRI.zip")


class Federal2021(RegionABC):
    """Regions for federal house of representatives election boundaries for 2021."""

    raw_geometry_url = FEDERAL_ELECTION_BOUNDARY_2021_URL

    @classproperty
    def id(cls) -> str:
        """Return the name for this region.

        Example
        -------
        `data.select(Region.FederalHouse2022.id)`
        """
        id = "federal_2021"
        return id

    @classproperty
    def raw_geometry_file(cls) -> str:
        """Get the path to the raw data shapefile."""
        raw_geometry_file = FEDERAL_ELECTION_BOUNDARY_2021_RAW_FILE
        return raw_geometry_file

    @classmethod
    def _transform_geometry_raw(cls, geometry_raw: st.GeoDataFrame) -> st.GeoDataFrame:
        """Transform data from raw shape.

        Returns
        -------
        st.GeoDataFrame: Processed
        ```
        shape: (151, 3)
        ┌──────────────┬─────────────────────────────────┬─────────────────────────────────┐
        │ federal_2021 ┆ metadata                        ┆ geometry                        │
        │ ---          ┆ ---                             ┆ ---                             │
        │ str          ┆ struct[4]                       ┆ binary                          │
        ╞══════════════╪═════════════════════════════════╪═════════════════════════════════╡
        │ Banks        ┆ {"Banks","Banks",49.47,0}       ┆ POLYGON Z ((151.129671 -33.973… │
        │ Barton       ┆ {"Barton","Barton",39.65,0}     ┆ POLYGON Z ((151.17424 -33.9249… │
        │ Bennelong    ┆ {"Bennelong","Bennelong",58.76… ┆ POLYGON Z ((151.159079 -33.798… │
        │ Berowra      ┆ {"Berowra","Berowra",741.64,0}  ┆ POLYGON Z ((151.284798 -33.572… │
        │ Blaxland     ┆ {"Blaxland","Blaxland",61.16,0… ┆ POLYGON Z ((151.044405 -33.842… │
        │ …            ┆ …                               ┆ …                               │
        │ O'Connor     ┆ {"O'Connor","O'Connor",1.1269e… ┆ MULTIPOLYGON Z (((129.002045 -… │
        │ Pearce       ┆ {"Pearce","Pearce",782.75,1063… ┆ POLYGON Z ((115.982838 -31.741… │
        │ Perth        ┆ {"Perth","Perth",79.92,116242}  ┆ POLYGON Z ((115.964319 -31.887… │
        │ Swan         ┆ {"Swan","Swan",150.89,114942}   ┆ POLYGON Z ((116.041017 -31.983… │
        │ Tangney      ┆ {"Tangney","Tangney",101.97,11… ┆ POLYGON Z ((115.951618 -32.082… │
        └──────────────┴─────────────────────────────────┴─────────────────────────────────┘

        ```
        """
        geometry_with_metadata = geometry_raw.select(
            pl.col("Elect_div").alias(cls.id),
            pl.struct(
                pl.col("Elect_div").alias(cls.name),
                pl.col("Sortname").alias("short_name"),
                pl.col("Area_SqKm").alias("area_sqkm"),
                pl.col("Actual").alias("actual"),
            ).alias("metadata"),
            pl.col("geometry"),
        )
        return geometry_with_metadata
