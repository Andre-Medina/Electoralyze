import polars as pl
import polars_st as st
from electoralyze.common.functools import classproperty
from electoralyze.region.region_abc import RegionABC

from .SA1_2021 import SA1_2021_RAW_FILE, SA1_2021_RAW_FILE_URL


class SA2_2021(RegionABC):
    """Regions for SA2_2021."""

    raw_geometry_url = SA1_2021_RAW_FILE_URL

    @classproperty
    def id(cls) -> str:
        """Return the name for this region.

        Example
        -------
        `data.select(Region.SA2_2021.id)`
        """
        id = "SA2_2021"
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
        geometry_filtered = geometry_raw.filter(~pl.col("SA2_CODE21").str.starts_with("Z")).select(
            pl.col("SA2_CODE21").cast(pl.Int64).alias(cls.id),
            pl.col("SA2_NAME21").cast(pl.String).alias(cls.name),
            "geometry",
        )

        geometry_grouped = geometry_filtered.group_by(cls.id).agg(
            pl.col(cls.name).first(),
            st.geom("geometry").st.union_all(),
        )

        geometry_with_metadata = geometry_grouped.select(
            pl.col(cls.id),
            pl.struct(pl.col(cls.name)).alias("metadata"),
            pl.col("geometry"),
        )

        return geometry_with_metadata
