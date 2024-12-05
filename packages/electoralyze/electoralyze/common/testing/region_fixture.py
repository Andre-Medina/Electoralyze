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

FOUR_SQUARE_REGION_ID = "region_a"
THREE_TRIANGLES_REGION_ID = "region_b"
THREE_RECTANGLE_REGION_ID = "region_c"

REGIONS = Literal[FOUR_SQUARE_REGION_ID, THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID]
REGION_IDS = [FOUR_SQUARE_REGION_ID, THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID]
REGION_JSONS = {
    FOUR_SQUARE_REGION_ID: {
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
        FOUR_SQUARE_REGION_ID: ["M", "N", "O", "P"],
        f"{FOUR_SQUARE_REGION_ID}_name": ["Mew", "New", "Omega", "Phi"],
        "extra": ["1", "2", "3", "4"],
        "geometry": [
            "POLYGON ((0 0, 0 1, -1 1, -1 0, 0 0))",
            "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
            "POLYGON ((0 0, 0 -1, -1 -1, -1 0, 0 0))",
            "POLYGON ((0 0, 0 -1, 1 -1, 1 0, 0 0))",
        ],
    },
    THREE_TRIANGLES_REGION_ID: {
        # Three Triangles
        # \─────────────/1\─────────────/
        #  \     B     / A \     C     /
        #   \         /     \         /
        # -2  \  -1   /   O   \   1   /  2
        #     \     /         \     /
        #      \   /           \   /
        #       \ /─────-1──────\ /
        THREE_TRIANGLES_REGION_ID: ["A", "B", "C"],
        f"{THREE_TRIANGLES_REGION_ID}_name": ["Alpha", "Bravo", "Charlie"],
        "extra": ["1", "2", "3"],
        "geometry": [
            "POLYGON ((-1 -1, 0 1, 1 -1, -1 -1))",
            "POLYGON ((-1 -1, 0 1, -2 1, -1 -1))",
            "POLYGON ((1 -1, 0 1, 2 1, 1 -1))",
        ],
    },
    THREE_RECTANGLE_REGION_ID: {
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
        THREE_RECTANGLE_REGION_ID: ["X", "Y", "Z"],
        f"{THREE_RECTANGLE_REGION_ID}_name": ["xi", "upsilon", "zeta"],
        "extra": ["5", "5", "5"],
        "geometry": [
            f"POLYGON ((-1 -1, 1 -1, 1 -{QUARTER}, -1 -{QUARTER}, -1 -1))",
            f"POLYGON ((-1 -{QUARTER}, 1 -{QUARTER}, 1 {QUARTER}, -1 {QUARTER}, -1 -{QUARTER}))",
            f"POLYGON ((-1 {QUARTER}, 1 {QUARTER}, 1 1, -1 1, -1 {QUARTER}))",
        ],
    },
}

REDISTRIBUTE_MAPPINGS = {
    tuple({FOUR_SQUARE_REGION_ID, THREE_TRIANGLES_REGION_ID}): pl.DataFrame(
        {
            FOUR_SQUARE_REGION_ID: ["M", "M", "N", "N", "O", "O", "P", "P", None, None],
            THREE_TRIANGLES_REGION_ID: ["A", "B", "A", "C", "A", "B", "A", "C", "C", "B"],
            "mapping": [0.25, 0.75, 0.25, 0.75, 0.75, 0.25, 0.75, 0.25, 1.0, 1.0],
        },
        schema=pl.Schema(
            {FOUR_SQUARE_REGION_ID: pl.String, THREE_TRIANGLES_REGION_ID: pl.String, "mapping": pl.Float64}
        ),
    ),
    tuple({FOUR_SQUARE_REGION_ID, THREE_RECTANGLE_REGION_ID}): pl.DataFrame(
        {
            FOUR_SQUARE_REGION_ID: ["M", "M", "N", "N", "O", "O", "P", "P"],
            THREE_RECTANGLE_REGION_ID: ["Y", "Z", "Y", "Z", "X", "Y", "X", "Y"],
            "mapping": [QUARTER, 1 - QUARTER, QUARTER, 1 - QUARTER, 1 - QUARTER, QUARTER, 1 - QUARTER, QUARTER],
        },
        schema=pl.Schema(
            {FOUR_SQUARE_REGION_ID: pl.String, THREE_RECTANGLE_REGION_ID: pl.String, "mapping": pl.Float64}
        ),
    ),
    tuple({THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID}): pl.DataFrame(
        {
            THREE_TRIANGLES_REGION_ID: ["A", "A", "A", "B", "B", "B", "C", "C", "C", "C", "B"],
            THREE_RECTANGLE_REGION_ID: ["X", "Y", "Z", "X", "Y", "Z", "X", "Y", "Z", None, None],
            "mapping": [1.21875, 0.5, 0.28125, 0.140625, 0.25, 0.609375, 0.140625, 0.25, 0.609375, 1.0, 1.0],
        },
        schema=pl.Schema(
            {THREE_TRIANGLES_REGION_ID: pl.String, THREE_RECTANGLE_REGION_ID: pl.String, "mapping": pl.Float64}
        ),
    ),
}


def get_true_redistribution(region_from: REGIONS, region_to: REGIONS) -> pl.DataFrame:
    """Get true region given two ids."""
    key_ = tuple({region_to, region_from})
    region_mapping = REDISTRIBUTE_MAPPINGS[key_]
    return region_mapping


class RegionMocked:
    """Class to type hint the mocked regions."""

    region_a: RegionABC
    region_b: RegionABC
    region_c: RegionABC

    def __init__(self, **regions: dict[REGIONS, RegionABC]):
        for region_id, region_ in regions.items():
            setattr(self, region_id, region_)

    def from_id(self, region_id: str) -> RegionABC:
        """Get a region from its id."""
        region_ = getattr(self, region_id)
        return region_

    def remove_processed_files(self) -> None:
        """Remove processed files."""
        for region_ in (self.region_a, self.region_b, self.region_c):
            region_.remove_processed_files()
            region_.cache_clear()


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
        region_class = create_fake_regions(temp_dir)
        yield region_class


def create_fake_regions(temp_dir: str):
    """Create fake regions.

    Example
    -------
    ```python
    >>> temp_dir = tempfile.TemporaryDirectory(delete = False)
    >>> fake_region = create_fake_regions(temp_dir.name)
    ...
    >>> import shutil
    >>> shutil.rmtree(temp_dir.name)
    ```
    """
    if not isinstance(temp_dir, str):
        raise TypeError("temp_dir must be a string")

    region_shape_file = {}
    for region_id in REGION_IDS:
        region_shape_file[region_id] = f"{temp_dir}/raw_geometry/{region_id}/shape.shp"
        os.makedirs(f"{temp_dir}/raw_geometry/{region_id}", exist_ok=True)

        region_gdf = pl.DataFrame(REGION_JSONS[region_id]).with_columns(geometry=st.from_wkt("geometry"))
        region_gdf.pipe(to_geopandas).to_file(region_shape_file[region_id], driver="ESRI Shapefile")

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

    region_classes = {}
    for region_id in REGION_IDS:

        def create_new_region(region_id_):
            class NewRegion(RegionMockedABC):
                """Mocked region."""

                @classproperty
                def id(self) -> str:
                    """Id for region."""
                    return region_id_

                @classproperty
                def raw_geometry_file(self) -> str:
                    """Raw file."""
                    return region_shape_file[region_id_]

            return NewRegion

        region_classes[region_id] = create_new_region(region_id)

    region_class = RegionMocked(**region_classes)

    for region_id in REGION_IDS:
        region_class.from_id(region_id).process_raw()

    return region_class


def read_true_geometry(region_id: REGIONS, /, *, raw: bool = False) -> st.GeoDataFrame:
    """Read true geometry from raws."""
    region_json = REGION_JSONS[region_id]
    geometry_raw = st.GeoDataFrame(region_json).select(region_id, "geometry")

    if raw:
        return geometry_raw

    geometry = geometry_raw.with_columns(st.geom("geometry").st.simplify(REGION_SIMPLIFY_TOLERANCE))
    return geometry


def read_true_metadata(region_id: REGIONS, /) -> pl.DataFrame:
    """Read true metadata from raws."""
    region_json = REGION_JSONS[region_id]
    region_name = list(REGION_JSONS[region_id].keys())[1]

    metadata_raw = (
        st.GeoDataFrame(region_json)
        .select(region_id, region_name, "extra")
        .with_columns(pl.col("extra").cast(pl.Int32))
    )

    return metadata_raw
