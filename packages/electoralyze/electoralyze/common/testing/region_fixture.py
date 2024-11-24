import os
import shutil
import tempfile

import polars as pl
import polars_st as st
import pytest
from electoralyze.common.functools import classproperty
from electoralyze.common.geometry import to_gpd_gdf
from electoralyze.region.region_abc import RegionABC

REGION_A_JSON = {
    "region_a": ["M", "N", "O", "P"],
    "region_a_name": ["Mew", "New", "Omega", "Phi"],
    "extra": ["1", "2", "3", "4"],
    "geometry": [
        "POLYGON ((0 0, 0 1, -1 1, -1 0, 0 0))",
        "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
        "POLYGON ((0 0, 0 -1, -1 -1, -1 0, 0 0))",
        "POLYGON ((0 0, 0 -1, 1 -1, 1 0, 0 0))",
    ],
}

REGION_B_JSON = {
    "region_b": ["A", "B", "C"],
    "region_b_name": ["Alpha", "Bravo", "Charlie"],
    "extra": ["1", "2", "3"],
    "geometry": [
        "POLYGON ((-1 -1, 0 1, 1 -1, -1 -1))",
        "POLYGON ((-1 -1, 0 1, -2 1, -1 -1))",
        "POLYGON ((1 -1, 0 1, 2 1, 1 -1))",
    ],
}


class RegionMocked:
    """Class to type hint the mocked regions."""

    RegionA: RegionABC
    RegionB: RegionABC

    def __init__(self, region_a, region_b):
        self.RegionA = region_a
        self.RegionB = region_b


@pytest.fixture(scope="module")
def region():
    """Fixture to create a region."""
    temp_dir = tempfile.TemporaryDirectory(delete=False)
    region_a_shape = f"{temp_dir.name}/raw_geometry/data_a/shape.shp"
    region_b_shape = f"{temp_dir.name}/raw_geometry/data_b/shape.shp"
    metadata_file_ = f"{temp_dir.name}" + "/metadata/{region}.parquet"
    geometry_file_ = f"{temp_dir.name}" + "/geometry/{region}.parquet"

    os.makedirs(f"{temp_dir.name}/raw_geometry/data_a", exist_ok=True)
    os.makedirs(f"{temp_dir.name}/raw_geometry/data_b", exist_ok=True)
    os.makedirs(f"{temp_dir.name}/metadata", exist_ok=True)
    os.makedirs(f"{temp_dir.name}/geometry", exist_ok=True)

    region_a_gdf = pl.DataFrame(REGION_A_JSON).with_columns(geometry=st.from_wkt("geometry"))
    region_a_gdf.pipe(to_gpd_gdf).to_file(region_a_shape, driver="ESRI Shapefile")
    region_b_gdf = pl.DataFrame(REGION_B_JSON).with_columns(geometry=st.from_wkt("geometry"))
    region_b_gdf.pipe(to_gpd_gdf).to_file(region_b_shape, driver="ESRI Shapefile")

    class RegionMockedABC(RegionABC):
        """Mocked region ABC."""

        @classproperty
        def metadata_file(self) -> str:
            """Get the path to the metadata file."""
            metadata_file = metadata_file_.format(region=self.id)
            return metadata_file

        @classproperty
        def geometry_file(self) -> str:
            """Get the path to the processed geometry file."""
            geometry_file = geometry_file_.format(region=self.id)
            return geometry_file

        @classmethod
        def _get_geometry_with_metadata(cls) -> st.GeoDataFrame:
            """Structure data."""
            geometry_raw = cls._get_geometry_raw()

            geometry_raw = geometry_raw.select(
                pl.col(cls.id),
                pl.struct(
                    pl.col(cls.name[0:10]).alias(cls.name),
                    pl.col("extra").cast(pl.Int32),
                ).alias("metadata"),
                pl.col("geometry"),
            )

            return geometry_raw

    class RegionA(RegionMockedABC):
        """RegionA for testing."""

        @classproperty
        def id(self) -> str:
            """Id for region_a."""
            return "region_a"

        @classproperty
        def raw_geometry_file(self) -> str:
            """Raw file."""
            return region_a_shape

    class RegionB(RegionMockedABC):
        """RegionB for testing."""

        @classproperty
        def id(self) -> str:
            """Id for region_b."""
            return "region_b"

        @classproperty
        def raw_geometry_file(self) -> str:
            """Raw file."""
            return region_b_shape

    region_class = RegionMocked(region_a=RegionA, region_b=RegionB)

    yield region_class

    shutil.rmtree(temp_dir.name)
