import os
from abc import ABC, abstractmethod

import geopandas as gpd
import polars as pl
import polars_st as st
import pyogrio
from cachetools import LRUCache, TTLCache, cached

from electoralyze.common.constants import ELECTORALYZE_DIR, REGION_SIMPLIFY_TOLERANCE
from electoralyze.common.functools import classproperty
from electoralyze.common.geometry import to_gpd_gdf, to_st_gdf

REDISTRIBUTE_FILE = os.path.join(ELECTORALYZE_DIR, "region/regions/{from}/redistribute/{to}.parquet")
GEOMETRY_SHAPE_FILE = os.path.join(ELECTORALYZE_DIR, "region/regions/{region}/geometry/{region}.parquet")
METADATA_FILE = os.path.join(ELECTORALYZE_DIR, "region/regions/{region}/metadata.parquet")


class RegionABC(ABC):
    """Regions."""

    @classproperty
    @abstractmethod
    def id(self) -> str:
        """Return the name for this region.

        Example
        -------
        `data.select(Region.SA1_2021.id)`
        """
        pass

    @classproperty
    def name(self) -> str:
        """Returns the corresponding name column.

        Example
        -------
        `data.select(Region.SA1_2021.name)`
        """
        name_column = f"{self.id}_name"
        return name_column

    #### READING #############
    @classproperty
    def geometry(cls) -> st.GeoDataFrame:
        """Read the simplified local geometry."""
        geometry = cls._geometry_cached()
        return geometry

    @classmethod
    @cached(LRUCache(maxsize=32))
    def _geometry_cached(cls) -> st.GeoDataFrame:
        """Actually reads and caches the data."""
        # geometry_read = pyogrio.read_dataframe(cls.geometry_file)
        geometry_read = gpd.read_parquet(cls.geometry_file)
        geometry = geometry_read.pipe(to_st_gdf)
        return geometry

    @classproperty
    def metadata(cls) -> pl.DataFrame:
        """Read the metadata locally."""
        metadata = cls._metadata_cached()
        return metadata

    @classmethod
    @cached(LRUCache(maxsize=32))
    def _metadata_cached(cls) -> pl.DataFrame:
        """Actually reads and caches the data."""
        metadata = pl.read_parquet(cls.metadata_file)
        return metadata

    #### FILES ################
    @classproperty
    @abstractmethod
    def raw_geometry_file(self) -> str:
        """Get the path to the raw data shapefile."""
        pass

    @classproperty
    def metadata_file(self) -> str:
        """Get the path to the metadata file."""
        metadata_file = METADATA_FILE.format(region=self.id)
        return metadata_file

    @classproperty
    def geometry_file(self) -> str:
        """Get the path to the processed geometry file."""
        geometry_file = GEOMETRY_SHAPE_FILE.format(region=self.id)
        return geometry_file

    #### PROCESSING #########

    @classmethod
    def process_raw(cls):
        """Extract and process raw data."""
        print("Loading raw...")
        geometry_raw = cls.get_raw_geometry()
        metadata = cls.get_raw_metadata()

        geometry = geometry_raw.with_columns(st.geom("geometry").st.simplify(REGION_SIMPLIFY_TOLERANCE)).pipe(
            to_gpd_gdf
        )

        print("Saving...")

        os.makedirs(cls.geometry_file.rsplit("/", maxsplit=1)[0], exist_ok=True)
        geometry.to_parquet(cls.geometry_file)

        os.makedirs(cls.metadata_file.rsplit("/", maxsplit=1)[0], exist_ok=True)
        metadata.write_parquet(cls.metadata_file)

        print("Done!")

    @classmethod
    def get_raw_geometry(cls) -> st.GeoDataFrame:
        """Get full raw geometry. Loads from raw file. may take a while."""
        geometry_with_metadata = cls._get_geometry_with_metadata()
        geometry: gpd.GeoDataFrame = geometry_with_metadata.select(cls.id, "geometry")
        return geometry

    @classmethod
    def get_raw_metadata(cls) -> pl.DataFrame:
        """Get raw metadata. Loads from raw file. may take a while."""
        geometry_with_metadata = cls._get_geometry_with_metadata()
        metadata = geometry_with_metadata.select(cls.id, "metadata").unnest("metadata")
        return metadata

    @classmethod
    @cached(TTLCache(maxsize=1, ttl=300))
    def _get_geometry_with_metadata(cls) -> st.GeoDataFrame:
        """Transform data from raw shape.

        Returns
        -------
        st.GeoDataFrame: Processed geometry data with three columns.
        - id columns: column with unique id for each row
        - geometry: geometries for each id
        - metadata: pl.struct column with any number of sub columns. metadata for each geometry.

        E.g.
        ```
        shape: (2_472, 3)
        ┌───────────┬────────────────────────────┬─────────────────────────────────┐
        │ SA2_2021  ┆ metadata                   ┆ geometry                        │
        │ ---       ┆ ---                        ┆ ---                             │
        │ i64       ┆ struct[1]                  ┆ binary                          │
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
        print("Extracting...")
        geometry_raw = cls._get_geometry_raw()
        print("Transforming...")
        geometry = cls._transform_geometry_raw(geometry_raw)
        return geometry

    @classmethod
    @abstractmethod
    def _transform_geometry_raw(cls, geometry_raw: st.GeoDataFrame) -> st.GeoDataFrame:
        """Abstract function to be overwritten by child classes. Transforms raw geometry into geometry with metadata.

        Returns
        -------
        st.GeoDataFrame: Processed geometry data with three columns.
        - id columns: column with unique id for each row
        - geometry: geometries for each id
        - metadata: pl.struct column with any number of sub columns. metadata for each geometry.
        """
        pass

    @classmethod
    def _get_geometry_raw(cls) -> st.GeoDataFrame:
        """Extract raw data from the raw shape file.

        Returns
        -------
        st.GeoDataFrame: In any format with any number columns. Should be accepted by `.transform`.
        """
        geometry_raw_gpd = pyogrio.read_dataframe(cls.raw_geometry_file)
        geometry_raw_st = to_st_gdf(geometry_raw_gpd)
        return geometry_raw_st