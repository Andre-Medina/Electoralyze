import polars as pl
import pytest
from electoralyze.common.testing.region_fixture import (
    REDISTRIBUTE_MAPPING_A_TO_B,
    REDISTRIBUTE_MAPPING_A_TO_C,
    REDISTRIBUTE_MAPPING_B_TO_C,
    RegionMocked,
)
from electoralyze.region.redistribute.mapping import (
    MAPPING_OPTIONS,
    _create_intersection_area_mapping,
    create_region_mapping_base,
    get_region_mapping_base,
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


@pytest.mark.parametrize(
    "_name, region_id_from, region_id_to, mapping_method, redistribute_with_full, expected, error",
    [
        ("a to b, ", "region_a", "region_b", "intersection_area", True, REDISTRIBUTE_MAPPING_A_TO_B, None),
        (
            "b to a: removes file, ",
            "region_b",
            "region_a",
            "intersection_area",
            None,
            None,
            FileNotFoundError,
        ),
        (
            "a to c: no file at all, ",
            "region_a",
            "region_c",
            "intersection_area",
            None,
            None,
            FileNotFoundError,
        ),
        (
            "b to c: using simplified, , ",
            "region_b",
            "region_c",
            "intersection_area",
            False,
            REDISTRIBUTE_MAPPING_B_TO_C,
            None,
        ),
        (
            "b to c: trying to use centroid, , ",
            "region_b",
            "region_c",
            "centroid_distance",
            False,
            None,
            NotImplementedError,
        ),
    ],
)
def test_get_region_mapping_base(
    region: RegionMocked,
    _name,
    region_id_from: str,
    region_id_to: str,
    mapping_method: MAPPING_OPTIONS,
    redistribute_with_full: bool | None,
    expected: pl.DataFrame,
    error: Exception,
):
    """Test region fixture keeps saved data."""
    if error is None:
        region_mapping_base = get_region_mapping_base(
            region_from=region.from_id(region_id_from),
            region_to=region.from_id(region_id_to),
            mapping_method=mapping_method,
            redistribute_with_full=redistribute_with_full,
        )
        pl.testing.assert_frame_equal(
            region_mapping_base,
            expected,
            check_column_order=False,
            check_row_order=False,
        )
    else:
        with pytest.raises(error):
            get_region_mapping_base(
                region_from=region.from_id(region_id_from),
                region_to=region.from_id(region_id_to),
                mapping_method=mapping_method,
                redistribute_with_full=redistribute_with_full,
            )


@pytest.mark.parametrize(
    "_name, region_id_from, region_id_to, mapping_method, redistribute_with_full, save_data, expected, error",
    [
        (
            "a to b, typical, ",
            "region_a",
            "region_b",
            "intersection_area",
            True,
            True,
            REDISTRIBUTE_MAPPING_A_TO_B,
            None,
        ),
        (
            "b to a: file still there, ",
            "region_b",
            "region_a",
            "intersection_area",
            None,
            False,
            REDISTRIBUTE_MAPPING_A_TO_B,
            None,
        ),
        (
            "a to c: no file after not saving, ",
            "region_a",
            "region_c",
            "intersection_area",
            None,
            False,
            None,
            FileNotFoundError,
        ),
        (
            "b to c: using simplified, ",
            "region_b",
            "region_c",
            "intersection_area",
            False,
            True,
            REDISTRIBUTE_MAPPING_B_TO_C,
            None,
        ),
    ],
)
def test_create_region_mapping_base(
    region: RegionMocked,
    _name,
    region_id_from: str,
    region_id_to: str,
    mapping_method: MAPPING_OPTIONS,
    redistribute_with_full: bool | None,
    save_data: bool,
    expected: pl.DataFrame,
    error: Exception,
):
    """Test region fixture keeps saved data."""
    # Create the region data
    create_region_mapping_base(
        region_from=region.from_id(region_id_from),
        region_to=region.from_id(region_id_to),
        mapping=mapping_method,
        redistribute_with_full=True if redistribute_with_full is None else redistribute_with_full,
        save_data=save_data,
    )

    if error is None:
        # If no error, get file and check its correct
        region_mapping_base = get_region_mapping_base(
            region_from=region.from_id(region_id_from),
            region_to=region.from_id(region_id_to),
            mapping_method=mapping_method,
            redistribute_with_full=redistribute_with_full,
        )
        pl.testing.assert_frame_equal(
            region_mapping_base,
            expected,
            check_column_order=False,
            check_row_order=False,
        )
    else:
        # If error, check it gets raised.
        with pytest.raises(error):
            get_region_mapping_base(
                region_from=region.from_id(region_id_from),
                region_to=region.from_id(region_id_to),
                mapping_method=mapping_method,
                redistribute_with_full=redistribute_with_full,
            )
