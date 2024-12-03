import os

# import shutil
import tempfile
from typing import Literal

import polars as pl
import polars_st as st
import pytest
from electoralyze.common.constants import REGION_SIMPLIFY_TOLERANCE
from electoralyze.common.functools import classproperty
from electoralyze.common.geometry import to_geopandas
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

REGIONS = Literal["region_a", "region_b"]


class RegionMocked:
    """Class to type hint the mocked regions."""

    RegionA: RegionABC
    RegionB: RegionABC

    def __init__(self, region_a, region_b):
        self.RegionA = region_a
        self.RegionB = region_b


@pytest.fixture(scope="session")
def region():
    """Fixture to create a mocked region for testing.

    Example
    -------
    ```python
    def test_region(region):
        assert region.RegionA.id == "region_a"
        assert region.RegionB.id == "region_b"
    ```
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        region_a_shape = f"{temp_dir}/raw_geometry/data_a/shape.shp"
        region_b_shape = f"{temp_dir}/raw_geometry/data_b/shape.shp"
        metadata_file_ = f"{temp_dir}" + "/metadata/{region}.parquet"
        geometry_file_ = f"{temp_dir}" + "/geometry/{region}.parquet"

        os.makedirs(f"{temp_dir}/raw_geometry/data_a", exist_ok=True)
        os.makedirs(f"{temp_dir}/raw_geometry/data_b", exist_ok=True)
        os.makedirs(f"{temp_dir}/metadata", exist_ok=True)
        os.makedirs(f"{temp_dir}/geometry", exist_ok=True)

        region_a_gdf = pl.DataFrame(REGION_A_JSON).with_columns(geometry=st.from_wkt("geometry"))
        region_a_gdf.pipe(to_geopandas).to_file(region_a_shape, driver="ESRI Shapefile")
        region_b_gdf = pl.DataFrame(REGION_B_JSON).with_columns(geometry=st.from_wkt("geometry"))
        region_b_gdf.pipe(to_geopandas).to_file(region_b_shape, driver="ESRI Shapefile")

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
            def _transform_geometry_raw(cls, geometry_raw: st.GeoDataFrame) -> st.GeoDataFrame:
                """Structure data."""
                geometry_with_metadata = geometry_raw.select(
                    pl.col(cls.id),
                    pl.struct(
                        pl.col(cls.name[0:10]).alias(cls.name),
                        pl.col("extra").cast(pl.Int32),
                    ).alias("metadata"),
                    pl.col("geometry"),
                )

                return geometry_with_metadata

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

    # FIXME: This should be auto deleted. issue #9
    # shutil.rmtree(temp_dir)


def read_true_geometry(region_id: REGIONS, /, *, raw: bool = False) -> st.GeoDataFrame:
    """Read true geometry from raws."""
    match region_id:
        case "region_a":
            region_json = REGION_A_JSON
        case "region_b":
            region_json = REGION_B_JSON
        case _:
            raise ValueError(f"Unknown region {region_id}")

    geometry_raw = st.GeoDataFrame(region_json).select(region_id, "geometry")

    if raw:
        return geometry_raw

    geometry = geometry_raw.with_columns(st.geom("geometry").st.simplify(REGION_SIMPLIFY_TOLERANCE))
    return geometry


def read_true_metadata(region_id: REGIONS, /) -> pl.DataFrame:
    """Read true metadata from raws."""
    match region_id:
        case "region_a":
            region_json = REGION_A_JSON
            region_name = "region_a_name"
        case "region_b":
            region_json = REGION_B_JSON
            region_name = "region_b_name"
        case _:
            raise ValueError(f"Unknown region {region_id}")

    metadata_raw = (
        st.GeoDataFrame(region_json)
        .select(region_id, region_name, "extra")
        .with_columns(pl.col("extra").cast(pl.Int32))
    )

    return metadata_raw
