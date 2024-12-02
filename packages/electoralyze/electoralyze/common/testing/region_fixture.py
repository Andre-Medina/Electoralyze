import os
import tempfile
from typing import Literal

import polars as pl
import polars_st as st
import pytest
from electoralyze.common.constants import REGION_SIMPLIFY_TOLERANCE
from electoralyze.common.functools import classproperty
from electoralyze.common.geometry import to_geopandas
from electoralyze.region.region_abc import RegionABC

QUARTER = 1 / 4

REGION_A_JSON = {
    # Four quadrants
    # ┌─────────1─────────┐
    # │         │         │
    # │    M    │    N    │
    # │         │         │
    # -1─────────O─────────1
    # │         │         │
    # │    O    │    P    │
    # │         │         │
    # └────────-1─────────┘
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
    # Three Triangles
    # \─────────────/1\─────────────/
    #  \     B     / A \     C     /
    #   \         /     \         /
    # -2  \  -1   /   O   \   1   /  2
    #     \     /         \     /
    #      \   /           \   /
    #       \ /─────-1──────\ /
    "region_b": ["A", "B", "C"],
    "region_b_name": ["Alpha", "Bravo", "Charlie"],
    "extra": ["1", "2", "3"],
    "geometry": [
        "POLYGON ((-1 -1, 0 1, 1 -1, -1 -1))",
        "POLYGON ((-1 -1, 0 1, -2 1, -1 -1))",
        "POLYGON ((1 -1, 0 1, 2 1, 1 -1))",
    ],
}

REGION_C_JSON = {
    # Three horizontal rectangles
    # ┌─────────1─────────┐
    # │    Z              │
    # ├───────────────────┤
    # │                   │
    # -1    Y    O         1
    # │                   │
    # ├───────────────────┤
    # │    X              │
    # └────────-1─────────┘
    "region_c": ["X", "Y", "Z"],
    "region_c_name": ["xi", "upsilon", "zeta"],
    "extra": ["5", "5", "5"],
    "geometry": [
        f"POLYGON ((-1 -1, 1 -1, 1 -{QUARTER}, -1 -{QUARTER}, -1 -1))",
        f"POLYGON ((-1 -{QUARTER}, 1 -{QUARTER}, 1 {QUARTER}, -1 {QUARTER}, -1 -{QUARTER}))",
        f"POLYGON ((-1 {QUARTER}, 1 {QUARTER}, 1 1, -1 1, -1 {QUARTER}))",
    ],
}

REGIONS = Literal["region_a", "region_b", "region_c"]

REDISTRIBUTE_MAPPING_A_TO_B = pl.DataFrame(
    {
        "region_a": ["M", "M", "N", "N", "O", "O", "P", "P", None, None],
        "region_b": ["A", "B", "A", "C", "A", "B", "A", "C", "C", "B"],
        "mapping": [0.25, 0.75, 0.25, 0.75, 0.75, 0.25, 0.75, 0.25, 1.0, 1.0],
    },
    schema=pl.Schema({"region_a": pl.String, "region_b": pl.String, "mapping": pl.Float64}),
)
REDISTRIBUTE_MAPPING_A_TO_C = pl.DataFrame(
    {
        "region_a": ["M", "M", "N", "N", "O", "O", "P", "P"],
        "region_c": ["Y", "Z", "Y", "Z", "X", "Y", "X", "Y"],
        "mapping": [QUARTER, 1 - QUARTER, QUARTER, 1 - QUARTER, 1 - QUARTER, QUARTER, 1 - QUARTER, QUARTER],
    },
    schema=pl.Schema({"region_a": pl.String, "region_c": pl.String, "mapping": pl.Float64}),
)

REDISTRIBUTE_MAPPING_B_TO_C = pl.DataFrame(
    {
        "region_b": ["A", "A", "A", "B", "B", "B", "C", "C", "C", "C", "B"],
        "region_c": ["X", "Y", "Z", "X", "Y", "Z", "X", "Y", "Z", None, None],
        "mapping": [1.21875, 0.5, 0.28125, 0.140625, 0.25, 0.609375, 0.140625, 0.25, 0.609375, 1.0, 1.0],
    },
    schema=pl.Schema({"region_b": pl.String, "region_c": pl.String, "mapping": pl.Float64}),
)


class RegionMocked:
    """Class to type hint the mocked regions."""

    region_a: RegionABC
    region_b: RegionABC
    region_c: RegionABC

    def __init__(self, *, region_a, region_b, region_c):
        self.region_a = region_a
        self.region_b = region_b
        self.region_c = region_c

    def from_id(self, region_id: str) -> RegionABC:
        """Get a region from its id."""
        region_ = getattr(self, region_id)
        return region_

    def remove_processed_files(self) -> None:
        """Remove processed files."""
        for region_ in (self.region_a, self.region_b, self.region_c):
            region_.remove_processed_files()


@pytest.fixture(scope="module")
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
        region_c_shape = f"{temp_dir}/raw_geometry/data_c/shape.shp"

        os.makedirs(f"{temp_dir}/raw_geometry/data_a", exist_ok=True)
        os.makedirs(f"{temp_dir}/raw_geometry/data_b", exist_ok=True)
        os.makedirs(f"{temp_dir}/raw_geometry/data_c", exist_ok=True)

        region_a_gdf = pl.DataFrame(REGION_A_JSON).with_columns(geometry=st.from_wkt("geometry"))
        region_a_gdf.pipe(to_geopandas).to_file(region_a_shape, driver="ESRI Shapefile")
        region_b_gdf = pl.DataFrame(REGION_B_JSON).with_columns(geometry=st.from_wkt("geometry"))
        region_b_gdf.pipe(to_geopandas).to_file(region_b_shape, driver="ESRI Shapefile")
        region_c_gdf = pl.DataFrame(REGION_C_JSON).with_columns(geometry=st.from_wkt("geometry"))
        region_c_gdf.pipe(to_geopandas).to_file(region_c_shape, driver="ESRI Shapefile")

        class RegionMockedABC(RegionABC):
            """Mocked region ABC."""

            _root_dir = temp_dir

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

        class RegionC(RegionMockedABC):
            """RegionC for testing."""

            @classproperty
            def id(self) -> str:
                """Id for region_c."""
                return "region_c"

            @classproperty
            def raw_geometry_file(self) -> str:
                """Raw file."""
                return region_c_shape

        region_class = RegionMocked(region_a=RegionA, region_b=RegionB, region_c=RegionC)

        region_class.region_a.process_raw()
        region_class.region_b.process_raw()
        region_class.region_c.process_raw()

        yield region_class


def read_true_geometry(region_id: REGIONS, /, *, raw: bool = False) -> st.GeoDataFrame:
    """Read true geometry from raws."""
    match region_id:
        case "region_a":
            region_json = REGION_A_JSON
        case "region_b":
            region_json = REGION_B_JSON
        case "region_c":
            region_json = REGION_C_JSON
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
        case "region_c":
            region_json = REGION_C_JSON
            region_name = "region_c_name"
        case _:
            raise ValueError(f"Unknown region {region_id}")

    metadata_raw = (
        st.GeoDataFrame(region_json)
        .select(region_id, region_name, "extra")
        .with_columns(pl.col("extra").cast(pl.Int32))
    )

    return metadata_raw
