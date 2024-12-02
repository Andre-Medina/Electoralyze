import polars as pl
import pytest
from electoralyze.common.testing.region_fixture import RegionMocked
from electoralyze.region.redistribute.mapping import (
    _create_intersection_area_mapping,
    # create_region_mapping_base,
)
from polars import testing  # noqa: F401


@pytest.mark.parametrize(
    "_name, region_id_from, region_id_to, expected",
    [
        (
            "create_region_mapping_base",
            "region_a",
            "region_b",
            pl.DataFrame(
                {
                    "region_a": ["M", "M", "N", "N", "O", "O", "P", "P", None, None],
                    "region_b": ["A", "B", "A", "C", "A", "B", "A", "C", "C", "B"],
                    "mapping": [0.25, 0.75, 0.25, 0.75, 0.75, 0.25, 0.75, 0.25, 1.0, 1.0],
                },
                schema=pl.Schema({"region_a": pl.String, "region_b": pl.String, "mapping": pl.Float64}),
            ),
        ),
    ],
)
def test_region_fixture_still_processed(
    _name: str, region_id_from: str, region_id_to: str, expected: pl.DataFrame, region: RegionMocked
):
    """Test region fixture keeps saved data."""
    intersection_area_mapping = _create_intersection_area_mapping(
        geometry_from=getattr(region, region_id_from).geometry,
        geometry_to=getattr(region, region_id_to).geometry,
    )
    pl.testing.assert_frame_equal(
        intersection_area_mapping.sort(["region_a", "region_b"]),
        expected.sort(["region_a", "region_b"]),
    )
