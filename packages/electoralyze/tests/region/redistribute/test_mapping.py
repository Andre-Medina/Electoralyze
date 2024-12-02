import polars as pl
import pytest
from electoralyze.common.testing.region_fixture import (
    REDISTRIBUTE_MAPPING_A_TO_B,
    REDISTRIBUTE_MAPPING_A_TO_C,
    REDISTRIBUTE_MAPPING_B_TO_C,
    RegionMocked,
)
from electoralyze.region.redistribute.mapping import (
    _create_intersection_area_mapping,
    # create_region_mapping_base,
)
from polars import testing  # noqa: F401


@pytest.mark.parametrize(
    "_name, region_id_from, region_id_to, expected",
    [
        ("a to b, ", "region_a", "region_b", REDISTRIBUTE_MAPPING_A_TO_B),
        ("b to a, ", "region_b", "region_a", REDISTRIBUTE_MAPPING_A_TO_B),
        ("a to c, ", "region_a", "region_c", REDISTRIBUTE_MAPPING_A_TO_C),
        ("b to c, ", "region_b", "region_c", REDISTRIBUTE_MAPPING_B_TO_C),
    ],
)
def test_create_intersection_area_mapping(
    _name: str, region_id_from: str, region_id_to: str, expected: pl.DataFrame, region: RegionMocked
):
    """Test region fixture keeps saved data."""
    intersection_area_mapping = _create_intersection_area_mapping(
        geometry_from=region.from_id(region_id_from).geometry,
        geometry_to=region.from_id(region_id_to).geometry,
    )
    pl.testing.assert_frame_equal(
        intersection_area_mapping,
        expected,
        check_column_order=False,
        check_row_order=False,
    )
