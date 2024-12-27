import os
from abc import ABC, abstractmethod

import geopandas as gpd
import polars as pl
import polars_st as st
import pyogrio
from cachetools import LRUCache, TTLCache, cached

from electoralyze.common.constants import REGION_SIMPLIFY_TOLERANCE, ROOT_DIR
from electoralyze.common.files import create_path, download_file
from electoralyze.common.functools import classproperty
from electoralyze.common.geometry import to_geopandas, to_geopolars

GEOMETRY_FILE = "{root_dir}/data/regions/{region}/geometry.parquet"
METADATA_FILE = "{root_dir}/data/regions/{region}/metadata.parquet"
_REDISTRIBUTE_FILE = "{root_dir}/data/regions/redistribute/{{mapping}}/{{region_a}}_{{region_b}}.parquet"


FULL_GEOMETRY_TTL_S = 900
BASE_DOWNLOAD_TIMEOUT = 60


class RegionABC(ABC):
    """Abstract base class for a region.

    To create a region you must:

    - Create a file in `electoralyze/region/regions/` with the name of the region
    - Create a child class of `RegionABC`
    - Overwrite the following methods, more info can be found in each of their doc strings.
      - `id`: Give the region an 'id' which will be used as the column name for the ids.
      - `raw_geometry_file`: Returns the path to the raw geometries.
      - `_transform_geometry_raw`: Takes raw geometry and processes it.
    - Refer to the newly created region child class in the `electoralyze/region/__init__.py` file.

    Example
    -------
    Walking through how a new region named `SA2_2021` would be added.

    First create the file `electoralyze/region/regions/SA2_2021.py` and add the corresponding class.
    ```python
        from electoralyze.region.region_abc import RegionABC
        ...
        class SA2_2021(RegionABC):

            @classproperty
            def id(cls) -> str:
                \"\"\"Return the name for this region.\"\"\"
                id = "SA2_2021"
                return id

            @classproperty
            def raw_geometry_file(cls) -> str:
                \"\"\"Get the path to the raw data shapefile.\"\"\"
                raw_geometry_file = os.path.join(ROOT_DIR, "data/raw/ASGA/2021/SA1/SA1_2021_AUST_GDA2020.shp")
                return raw_geometry_file

            @classmethod
            def _transform_geometry_raw(cls, geometry_raw: st.GeoDataFrame) -> st.GeoDataFrame:
                \"\"\"Transform data from raw shape.\"\"\"

                geometry_with_metadata = (
                    geometry_raw
                    # Some processing here
                )

                return geometry_with_metadata
    ```

    Reference the new class in `electoralyze/region/__init__.py`
    ```python
        from .regions.SA1_2021 import SA1_2021
        from .regions.SA2_2021 import SA2_2021

        __all__ = ["SA1_2021", "SA2_2021"]
    ```

    Now test all the functions work by using
    ```python
        from electoralyze import region

        # Check names and file paths
        print(region.SA2_2021.id)
        print(region.SA2_2021.name)
        print(region.SA2_2021.raw_geometry_file)
        print(region.SA2_2021.geometry_file)
        print(region.SA2_2021.metadata_file)

        # Check reading raw data
        print(region.SA2_2021.get_raw_geometry())
        print(region.SA2_2021.get_raw_metadata())

        # Process raw
        region.SA2_2021.process_raw()

        # Check the data processed and saved correctly
        print(region.SA2_2021.metadata)
        print(region.SA2_2021.geometry)
    ```

    Add processing steps to `/modelling/processing_raw_data/processing_regions.ipynb`.

    Can also consider adding tests
    - Integration tests in `tests/integration/test_region.py: test_region_process_raw`.
    - Testing some region basics in `tests/region/test_region_abc.py: test_true_region_id_and_name`.
    """

    _root_dir: str = ROOT_DIR
    raw_geometry_url: str
    timeout: int = BASE_DOWNLOAD_TIMEOUT

    @classproperty
    @abstractmethod
    def id(cls) -> str:
        """Return the name for this region.

        Example
        -------
        `data.select(Region.SA1_2021.id)`
        """
        pass

    @classproperty
    def name(cls) -> str:
        """Returns the corresponding name column.

        Example
        -------
        `data.select(Region.SA1_2021.name)`
        """
        name_column = f"{cls.id}_name"
        return name_column

    #### READING #############
    @classproperty
    def geometry(cls) -> st.GeoDataFrame:
        """Geometry for this region, linking each region id to the polygon.

        Reads the simplified local geometry.

        Returns
        -------
        e.g.
        ```python
        >>> region.SA2_2021.geometry
        shape: (2_472, 2)
        ┌───────────┬─────────────────────────────────┐
        │ SA2_2021  ┆ geometry                        │
        │ ---       ┆ ---                             │
        │ i64       ┆ binary                          │
        ╞═══════════╪═════════════════════════════════╡
        │ 305031128 ┆ POLYGON ((153.040413 -27.44932… │
        │ 511041289 ┆ POLYGON ((114.921946 -29.27032… │
        │ 211051285 ┆ POLYGON ((145.411636 -37.79203… │
        │ 110011188 ┆ POLYGON ((151.135773 -30.28397… │
        │ 121011687 ┆ POLYGON ((151.209539 -33.80446… │
        │ …         ┆ …                               │
        │ 212031458 ┆ POLYGON ((145.279819 -38.06734… │
        │ 307011178 ┆ POLYGON ((150.400255 -26.97881… │
        │ 316071546 ┆ POLYGON ((152.976155 -26.61877… │
        │ 202021026 ┆ POLYGON ((144.421959 -36.81470… │
        │ 208021181 ┆ POLYGON ((145.078188 -37.88713… │
        └───────────┴─────────────────────────────────┘
        ```
        """
        geometry = cls._geometry_cached()
        return geometry

    @classmethod
    @cached(LRUCache(maxsize=32))
    def _geometry_cached(cls) -> st.GeoDataFrame:
        """Actually reads and caches the data."""
        # geometry_read = pyogrio.read_dataframe(cls.geometry_file)
        geometry_read = gpd.read_parquet(cls.geometry_file)
        geometry = geometry_read.pipe(to_geopolars)
        return geometry

    @classproperty
    def metadata(cls) -> pl.DataFrame:
        """Metadata for this region, linking each region id to more info e.g. region names.

        Reads the processed metadata stored locally.

        Returns
        -------
        e.g.
        ```python
        >>> region.SA2_2021.metadata
        shape: (2_472, 2)
        ┌───────────┬────────────────────────────┐
        │ SA2_2021  ┆ SA2_2021_name              │
        │ ---       ┆ ---                        │
        │ i64       ┆ str                        │
        ╞═══════════╪════════════════════════════╡
        │ 305031128 ┆ Newstead - Bowen Hills     │
        │ 511041289 ┆ Irwin                      │
        │ 211051285 ┆ Wandin - Seville           │
        │ 110011188 ┆ Armidale Surrounds - South │
        │ 121011687 ┆ Willoughby                 │
        │ …         ┆ …                          │
        │ 212031458 ┆ Narre Warren South - West  │
        │ 307011178 ┆ Tara                       │
        │ 316071546 ┆ Diddillibah - Rosemount    │
        │ 202021026 ┆ Bendigo Surrounds - South  │
        │ 208021181 ┆ Murrumbeena                │
        └───────────┴────────────────────────────┘
        ```
        """
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
    def raw_geometry_file(cls) -> str:
        """Get the path to the raw data shapefile."""
        pass

    @classproperty
    def metadata_file(cls) -> str:
        """Get the path to the metadata file."""
        metadata_file = METADATA_FILE.format(root_dir=cls._root_dir, region=cls.id)
        return metadata_file

    @classproperty
    def geometry_file(cls) -> str:
        """Get the path to the processed geometry file."""
        geometry_file = GEOMETRY_FILE.format(root_dir=cls._root_dir, region=cls.id)
        return geometry_file

    @classproperty
    def redistribute_file(cls) -> str:
        """Redistribute file, still needs to be formatted with other variables.

        Needed to be defined here to pass `cls._root_dir`.
        """
        redistribute_file = _REDISTRIBUTE_FILE.format(root_dir=cls._root_dir)
        return redistribute_file

    #### PROCESSING #########

    @classmethod
    def process_raw(cls, *, force_new: bool = False, download: bool = True) -> None:
        """Extract, transform and save the raw data to create data for `cls.geometry` and `cls.metadata`.

        Parameters
        ----------
        force_new (bool, optional): If True, will force a new download of the raw data. Defaults to False.
        download (bool, optional): If True, will download the raw data. Defaults to True.

        Returns
        -------
        None, updates `cls.geometry` and `cls.metadata`

        """
        if download or force_new:
            print("Downloading raw...")
            cls.download_data(force_new=force_new)

        print("Loading raw...")
        geometry_raw = cls.get_raw_geometry()
        metadata_raw = cls.get_raw_metadata()

        geometry = (
            geometry_raw.with_columns(st.geom("geometry").st.simplify(REGION_SIMPLIFY_TOLERANCE))
            .sort(cls.id)
            .pipe(to_geopandas)
        )
        metadata = metadata_raw.sort(cls.id)

        print("Saving...")

        create_path(cls.geometry_file)
        geometry.to_parquet(cls.geometry_file)

        create_path(cls.metadata_file)
        metadata.write_parquet(cls.metadata_file)

        print("Done!")

    @classmethod
    def download_data(cls, *, force_new: bool = False):
        """Download the raw data from the source."""
        if (not force_new) and (os.path.exists(cls.raw_geometry_file)):
            return
        download_file(cls.raw_geometry_url, cls.raw_geometry_file, timeout=cls.timeout)

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
    @cached(TTLCache(maxsize=1, ttl=FULL_GEOMETRY_TTL_S))
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
        if not os.path.exists(cls.raw_geometry_file):
            raise FileNotFoundError(f"File not found: {cls.raw_geometry_file!r}")

        geometry_raw_gpd = pyogrio.read_dataframe(cls.raw_geometry_file)
        geometry_raw_st = to_geopolars(geometry_raw_gpd)
        return geometry_raw_st

    ### UTILS ########

    @classmethod
    @cached(LRUCache(maxsize=32))
    def get_ids(cls) -> set:
        """Gets set of all ids for this region."""
        ids = set(cls.metadata[cls.id].unique().to_list())
        return ids

    @classmethod
    def remove_processed_files(cls):
        """Remove processed files."""
        if os.path.isfile(cls.geometry_file):
            os.remove(cls.geometry_file)
        if os.path.isfile(cls.metadata_file):
            os.remove(cls.metadata_file)
        cls.cache_clear()

    @classmethod
    def cache_clear(cls):
        """Clears the cache of class methods where data is cached."""
        cls._geometry_cached.cache_clear()
        cls._metadata_cached.cache_clear()
        cls._get_geometry_with_metadata.cache_clear()
