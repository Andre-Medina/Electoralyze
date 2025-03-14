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

FOUR_SQUARE_REGION_ID = "quadrant"
THREE_TRIANGLES_REGION_ID = "triangle"
THREE_RECTANGLE_REGION_ID = "rectangle"
ONE_SQUARE_REGION_ID = "square"
LEFT_RIGHT_REGION_ID = "l_and_r"
FAR_RIGHT_REGION_ID = "far_right"

REGIONS = Literal[
    FOUR_SQUARE_REGION_ID,
    THREE_TRIANGLES_REGION_ID,
    THREE_RECTANGLE_REGION_ID,
    ONE_SQUARE_REGION_ID,
    LEFT_RIGHT_REGION_ID,
    FAR_RIGHT_REGION_ID,
]
REGION_IDS = [
    FOUR_SQUARE_REGION_ID,
    THREE_TRIANGLES_REGION_ID,
    THREE_RECTANGLE_REGION_ID,
    ONE_SQUARE_REGION_ID,
    LEFT_RIGHT_REGION_ID,
    FAR_RIGHT_REGION_ID,
]
REGION_JSONS = {
    ONE_SQUARE_REGION_ID: {
        #  One square region.
        #  ┌─────────4─────────┐
        #  │                   │
        #  │    O              │
        #  │                   │
        # -4         0         4
        #  │                   │
        #  │                   │
        #  │                   │
        #  └────────-4─────────┘
        ONE_SQUARE_REGION_ID: ["main"],
        f"{ONE_SQUARE_REGION_ID}_name": ["main"],
        "extra": ["1000"],
        "geometry": [
            "POLYGON ((-4 -4, 4 -4, 4 4, -4 4, -4 -4))",
        ],
    },
    LEFT_RIGHT_REGION_ID: {
        #  Left and right sections.
        #  ┌─────────4─────────┐
        #  │         │         │
        #  │    L    │    R    │
        #  │         │         │
        # -4         0         4
        #  │         │         │
        #  │         │         │
        #  │         │         │
        #  └────────-4─────────┘
        LEFT_RIGHT_REGION_ID: ["L", "R"],
        f"{LEFT_RIGHT_REGION_ID}_name": ["Left", "Right"],
        "extra": ["-100", "100"],
        "geometry": [
            "POLYGON ((-4 -4, 0 -4, 0 4, -4 4, -4 -4))",
            "POLYGON ((0 -4, 4 -4, 4 4, 0 4, 0 -4))",
        ],
    },
    FOUR_SQUARE_REGION_ID: {
        #  Four quadrant
        #  ┌─────────4─────────┐
        #  │         │         │
        #  │    M    │    N    │
        #  │         │         │
        # -4─────────0─────────4
        #  │         │         │
        #  │    O    │    P    │
        #  │         │         │
        #  └────────-4─────────┘
        FOUR_SQUARE_REGION_ID: ["M", "N", "O", "P"],
        f"{FOUR_SQUARE_REGION_ID}_name": ["Mew", "New", "Omega", "Phi"],
        "extra": ["1", "2", "3", "4"],
        "geometry": [
            "POLYGON ((0 0, 0 4, -4 4, -4 0, 0 0))",
            "POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))",
            "POLYGON ((0 0, 0 -4, -4 -4, -4 0, 0 0))",
            "POLYGON ((0 0, 0 -4, 4 -4, 4 0, 0 0))",
        ],
    },
    THREE_TRIANGLES_REGION_ID: {
        #  Three Triangles
        #  \─────────────/4\─────────────/
        #   \     B     / A \     C     /
        #    \         /     \         /
        # -8  \  -4   /   0   \   4   /  8
        #      \     /         \     /
        #       \   /           \   /
        #        \ /─────-4──────\ /
        THREE_TRIANGLES_REGION_ID: ["A", "B", "C"],
        f"{THREE_TRIANGLES_REGION_ID}_name": ["Alpha", "Bravo", "Charlie"],
        "extra": ["1", "2", "3"],
        "geometry": [
            "POLYGON ((-4 -4, 0 4, 4 -4, -4 -4))",
            "POLYGON ((-4 -4, 0 4, -8 4, -4 -4))",
            "POLYGON ((4 -4, 0 4, 8 4, 4 -4))",
        ],
    },
    THREE_RECTANGLE_REGION_ID: {
        #  Three horizontal rectangles
        #  ┌─────────4─────────┐
        #  │    Z              │
        #  ├─────────2─────────┤
        #  │                   │
        # -4    Y    0         4
        #  │                   │
        #  ├─────────2─────────┤
        #  │    X              │
        #  └────────-4─────────┘
        THREE_RECTANGLE_REGION_ID: ["X", "Y", "Z"],
        f"{THREE_RECTANGLE_REGION_ID}_name": ["xi", "upsilon", "zeta"],
        "extra": ["5", "5", "5"],
        "geometry": [
            "POLYGON ((-4 -4, 4 -4, 4 -2, -4 -2, -4 -4))",
            "POLYGON ((-4 -2, 4 -2, 4 2, -4 2, -4 -2))",
            "POLYGON ((-4 2, 4 2, 4 4, -4 4, -4 2))",
        ],
    },
    FAR_RIGHT_REGION_ID: {
        #  Three horizontal rectangles
        #  3───────────────────────────────┐
        #  │   F1  │   F2  │   F3  │   F4  │
        #  │       │       │       │       │
        #  0───────3───────6───────9──────12
        #          |   F5  |
        #          |       |
        # -3       ---------
        FAR_RIGHT_REGION_ID: ["F1", "F2", "F3", "F4", "F5"],
        f"{FAR_RIGHT_REGION_ID}_name": ["a", "a", "a", "a", "a"],
        "extra": ["5", "5", "5", "5", "5"],
        "geometry": [
            "POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))",
            "POLYGON ((3 0, 6 0, 6 3, 3 3, 3 0))",
            "POLYGON ((6 0, 9 0, 9 3, 6 3, 6 0))",
            "POLYGON ((9 0, 12 0, 12 3, 9 3, 9 0))",
            "POLYGON ((3 0, 6 0, 6 -3, 3 -3, 3 0))",
        ],
    },
}

REDISTRIBUTE_MAPPINGS = {
    tuple({FOUR_SQUARE_REGION_ID, THREE_TRIANGLES_REGION_ID}): pl.DataFrame(
        [
            {FOUR_SQUARE_REGION_ID: "M", THREE_TRIANGLES_REGION_ID: "A", "mapping": 4.0},
            {FOUR_SQUARE_REGION_ID: "M", THREE_TRIANGLES_REGION_ID: "B", "mapping": 12.0},
            {FOUR_SQUARE_REGION_ID: "N", THREE_TRIANGLES_REGION_ID: "A", "mapping": 4.0},
            {FOUR_SQUARE_REGION_ID: "N", THREE_TRIANGLES_REGION_ID: "C", "mapping": 12.0},
            {FOUR_SQUARE_REGION_ID: "O", THREE_TRIANGLES_REGION_ID: "A", "mapping": 12.0},
            {FOUR_SQUARE_REGION_ID: "O", THREE_TRIANGLES_REGION_ID: "B", "mapping": 4.0},
            {FOUR_SQUARE_REGION_ID: "P", THREE_TRIANGLES_REGION_ID: "A", "mapping": 12.0},
            {FOUR_SQUARE_REGION_ID: "P", THREE_TRIANGLES_REGION_ID: "C", "mapping": 4.0},
            {FOUR_SQUARE_REGION_ID: None, THREE_TRIANGLES_REGION_ID: "B", "mapping": 16.0},
            {FOUR_SQUARE_REGION_ID: None, THREE_TRIANGLES_REGION_ID: "C", "mapping": 16.0},
        ],
        schema=pl.Schema(
            {FOUR_SQUARE_REGION_ID: pl.String, THREE_TRIANGLES_REGION_ID: pl.String, "mapping": pl.Float64}
        ),
    ),
    tuple({FOUR_SQUARE_REGION_ID, THREE_RECTANGLE_REGION_ID}): pl.DataFrame(
        [
            {FOUR_SQUARE_REGION_ID: "M", THREE_RECTANGLE_REGION_ID: "Y", "mapping": 8.0},
            {FOUR_SQUARE_REGION_ID: "M", THREE_RECTANGLE_REGION_ID: "Z", "mapping": 8.0},
            {FOUR_SQUARE_REGION_ID: "N", THREE_RECTANGLE_REGION_ID: "Y", "mapping": 8.0},
            {FOUR_SQUARE_REGION_ID: "N", THREE_RECTANGLE_REGION_ID: "Z", "mapping": 8.0},
            {FOUR_SQUARE_REGION_ID: "O", THREE_RECTANGLE_REGION_ID: "X", "mapping": 8.0},
            {FOUR_SQUARE_REGION_ID: "O", THREE_RECTANGLE_REGION_ID: "Y", "mapping": 8.0},
            {FOUR_SQUARE_REGION_ID: "P", THREE_RECTANGLE_REGION_ID: "X", "mapping": 8.0},
            {FOUR_SQUARE_REGION_ID: "P", THREE_RECTANGLE_REGION_ID: "Y", "mapping": 8.0},
        ],
        schema=pl.Schema(
            {FOUR_SQUARE_REGION_ID: pl.String, THREE_RECTANGLE_REGION_ID: pl.String, "mapping": pl.Float64}
        ),
    ),
    tuple({THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID}): pl.DataFrame(
        [
            {THREE_TRIANGLES_REGION_ID: "A", THREE_RECTANGLE_REGION_ID: "X", "mapping": 14.0},
            {THREE_TRIANGLES_REGION_ID: "B", THREE_RECTANGLE_REGION_ID: "X", "mapping": 1.0},
            {THREE_TRIANGLES_REGION_ID: "C", THREE_RECTANGLE_REGION_ID: "X", "mapping": 1.0},
            {THREE_TRIANGLES_REGION_ID: "A", THREE_RECTANGLE_REGION_ID: "Y", "mapping": 16.0},
            {THREE_TRIANGLES_REGION_ID: "B", THREE_RECTANGLE_REGION_ID: "Y", "mapping": 8.0},
            {THREE_TRIANGLES_REGION_ID: "C", THREE_RECTANGLE_REGION_ID: "Y", "mapping": 8.0},
            {THREE_TRIANGLES_REGION_ID: "A", THREE_RECTANGLE_REGION_ID: "Z", "mapping": 2.0},
            {THREE_TRIANGLES_REGION_ID: "B", THREE_RECTANGLE_REGION_ID: "Z", "mapping": 7.0},
            {THREE_TRIANGLES_REGION_ID: "C", THREE_RECTANGLE_REGION_ID: "Z", "mapping": 7.0},
            {THREE_TRIANGLES_REGION_ID: "B", THREE_RECTANGLE_REGION_ID: None, "mapping": 16.0},
            {THREE_TRIANGLES_REGION_ID: "C", THREE_RECTANGLE_REGION_ID: None, "mapping": 16.0},
        ],
        schema=pl.Schema(
            {THREE_TRIANGLES_REGION_ID: pl.String, THREE_RECTANGLE_REGION_ID: pl.String, "mapping": pl.Float64}
        ),
    ),
    tuple({FAR_RIGHT_REGION_ID, FOUR_SQUARE_REGION_ID}): pl.DataFrame(
        [
            {FAR_RIGHT_REGION_ID: "F1", FOUR_SQUARE_REGION_ID: "N", "mapping": 9.0},
            {FAR_RIGHT_REGION_ID: "F2", FOUR_SQUARE_REGION_ID: "N", "mapping": 3.0},
            {FAR_RIGHT_REGION_ID: None, FOUR_SQUARE_REGION_ID: "N", "mapping": 4.0},
            {FAR_RIGHT_REGION_ID: "F2", FOUR_SQUARE_REGION_ID: None, "mapping": 6.0},
            {FAR_RIGHT_REGION_ID: "F3", FOUR_SQUARE_REGION_ID: None, "mapping": 9.0},
            {FAR_RIGHT_REGION_ID: "F4", FOUR_SQUARE_REGION_ID: None, "mapping": 9.0},
            {FAR_RIGHT_REGION_ID: None, FOUR_SQUARE_REGION_ID: "M", "mapping": 16.0},
            {FAR_RIGHT_REGION_ID: None, FOUR_SQUARE_REGION_ID: "O", "mapping": 16.0},
            {FAR_RIGHT_REGION_ID: "F5", FOUR_SQUARE_REGION_ID: "P", "mapping": 3.0},
            {FAR_RIGHT_REGION_ID: "F5", FOUR_SQUARE_REGION_ID: None, "mapping": 6.0},
            {FAR_RIGHT_REGION_ID: None, FOUR_SQUARE_REGION_ID: "P", "mapping": 13.0},
        ],
        schema=pl.Schema({FAR_RIGHT_REGION_ID: pl.String, FOUR_SQUARE_REGION_ID: pl.String, "mapping": pl.Float64}),
    ),
}


def get_true_redistribution(region_from: REGIONS, region_to: REGIONS) -> pl.DataFrame:
    """Get true region given two ids."""
    key_ = tuple({region_to, region_from})
    region_mapping = REDISTRIBUTE_MAPPINGS[key_]
    return region_mapping


class RegionMocked:
    """Class to type hint the mocked regions."""

    quadrant: RegionABC
    triangle: RegionABC
    rectangle: RegionABC
    square: RegionABC
    l_and_r: RegionABC
    _RegionMockedABC: RegionABC

    def __init__(self, **regions: dict[REGIONS, RegionABC]):
        for region_id, region_ in regions.items():
            setattr(self, region_id, region_)

    def from_id(self, region_id: str) -> RegionABC:
        """Get a region from its id."""
        region_ = getattr(self, region_id)
        return region_

    def remove_processed_files(self) -> None:
        """Remove processed files."""
        for region_id in REGION_IDS:
            region_ = self.from_id(region_id)
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
            if cls.name[0:10] == cls.id:
                raise ValueError(f"First 10 chars of Name = ID for '{cls.id}', this will cause issues")

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
                    name_ = region_shape_file[region_id_]
                    return name_

            return NewRegion

        region_classes[region_id] = create_new_region(region_id)

    region_class = RegionMocked(_RegionMockedABC=RegionMockedABC, **region_classes)

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
