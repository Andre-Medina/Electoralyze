import os

import polars as pl
import polars_st as st
from electoralyze.common.constants import ROOT_DIR
from electoralyze.common.functools import classproperty
from electoralyze.region.region_abc import RegionABC

SA1_2021_RAW_FILE_URL = (
    "https://www.abs.gov.au/statistics/standards/"
    "australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads"
    "/digital-boundary-files/SA1_2021_AUST_SHP_GDA2020.zip"
)
SA1_2021_RAW_FILE = os.path.join(ROOT_DIR, "data/raw/SA1_2021_AUST_GDA2020.zip")


class SA1_2021(RegionABC):
    """Region for SA1_2021."""

    raw_geometry_url = SA1_2021_RAW_FILE_URL

    @classproperty
    def id(cls) -> str:
        """Return the name for this region.

        Example
        -------
        `data.select(Region.SA1_2021.id)`
        """
        id = "SA1_2021"
        return id

    @classproperty
    def raw_geometry_file(cls) -> str:
        """Get the path to the raw data shapefile."""
        raw_geometry_file = SA1_2021_RAW_FILE
        return raw_geometry_file

    @classmethod
    def _transform_geometry_raw(cls, geometry_raw: st.GeoDataFrame) -> st.GeoDataFrame:
        """Transform data from raw shape.

        Returns
        -------
        st.GeoDataFrame: Processed
        ```
        shape: (2_472, 3)
        ┌───────────┬────────────────────────────┬─────────────────────────────────┐
        │ SA2_2021  ┆ metadata                   ┆ geometry                        │
        │ ---       ┆ ---                        ┆ ---                             │
        │ i64       ┆ struct[1]                  ┆ str                             │
        ╞═══════════╪════════════════════════════╪═════════════════════════════════╡
        │ 119011656 ┆ {"Greenacre - South"}      ┆ POLYGON ((151.055825 -33.91691… │
        │ 101041017 ┆ {"Batemans Bay"}           ┆ POLYGON ((150.179359 -35.75046… │
        │ 213041577 ┆ {"Melton"}                 ┆ POLYGON ((144.597838 -37.67632… │
        │ 120011673 ┆ {"Rhodes"}                 ┆ POLYGON ((151.08541 -33.825075… │
        │ 306021150 ┆ {"Lamb Range"}             ┆ POLYGON ((145.716073 -16.94068… │
        │ …         ┆ …                          ┆ …                               │
        │ 211041269 ┆ {"Forest Hill"}            ┆ POLYGON ((145.174571 -37.83075… │
        │ 111031224 ┆ {"Hamilton - Broadmeadow"} ┆ POLYGON ((151.737571 -32.93625… │
        │ 211031450 ┆ {"Croydon - East"}         ┆ POLYGON ((145.282676 -37.79417… │
        │ 211041272 ┆ {"Vermont"}                ┆ POLYGON ((145.198972 -37.83051… │
        │ 304031093 ┆ {"Corinda"}                ┆ POLYGON ((152.990094 -27.54135… │
        └───────────┴────────────────────────────┴─────────────────────────────────┘
        ```
        """
        geometry_filtered = geometry_raw.filter(~pl.col("SA1_CODE21").str.starts_with("Z")).select(
            pl.col("SA1_CODE21").cast(pl.Int64).alias(cls.id),
            pl.col("SA1_CODE21").cast(pl.String).alias(cls.name),
            "geometry",
        )

        geometry_with_metadata = geometry_filtered.select(
            pl.col(cls.id),
            pl.struct(pl.col(cls.name)).alias("metadata"),
            pl.col("geometry"),
        )

        return geometry_with_metadata
