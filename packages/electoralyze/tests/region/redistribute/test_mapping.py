import polars as pl
import pytest
from electoralyze.common.testing.region_fixture import (
    FOUR_SQUARE_REGION_ID,
    THREE_RECTANGLE_REGION_ID,
    THREE_TRIANGLES_REGION_ID,
    RegionMocked,
    get_true_redistribution,
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
        (
            "a to b, ",
            FOUR_SQUARE_REGION_ID,
            THREE_TRIANGLES_REGION_ID,
            get_true_redistribution(THREE_TRIANGLES_REGION_ID, FOUR_SQUARE_REGION_ID),
        ),
        (
            "b to a, ",
            THREE_TRIANGLES_REGION_ID,
            FOUR_SQUARE_REGION_ID,
            get_true_redistribution(THREE_TRIANGLES_REGION_ID, FOUR_SQUARE_REGION_ID),
        ),
        (
            "a to c, ",
            FOUR_SQUARE_REGION_ID,
            THREE_RECTANGLE_REGION_ID,
            get_true_redistribution(THREE_RECTANGLE_REGION_ID, FOUR_SQUARE_REGION_ID),
        ),
        (
            "b to c, ",
            THREE_TRIANGLES_REGION_ID,
            THREE_RECTANGLE_REGION_ID,
            get_true_redistribution(THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID),
        ),
    ],
)
def test_create_intersection_area_mapping(
    _name: str, region_id_from: str, region_id_to: str, expected: pl.DataFrame, region: RegionMocked
):
    """Test cross section area is correct."""
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
        (
            "a to b, ",
            FOUR_SQUARE_REGION_ID,
            THREE_TRIANGLES_REGION_ID,
            "intersection_area",
            True,
            get_true_redistribution(THREE_TRIANGLES_REGION_ID, FOUR_SQUARE_REGION_ID),
            None,
        ),
        (
            "b to a: removes file, ",
            THREE_TRIANGLES_REGION_ID,
            FOUR_SQUARE_REGION_ID,
            "intersection_area",
            None,
            None,
            FileNotFoundError,
        ),
        (
            "a to c: no file at all, ",
            FOUR_SQUARE_REGION_ID,
            THREE_RECTANGLE_REGION_ID,
            "intersection_area",
            None,
            None,
            FileNotFoundError,
        ),
        (
            "b to c: using simplified, , ",
            THREE_TRIANGLES_REGION_ID,
            THREE_RECTANGLE_REGION_ID,
            "intersection_area",
            False,
            get_true_redistribution(THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID),
            None,
        ),
        (
            "b to c: trying to use centroid, , ",
            THREE_TRIANGLES_REGION_ID,
            THREE_RECTANGLE_REGION_ID,
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
            FOUR_SQUARE_REGION_ID,
            THREE_TRIANGLES_REGION_ID,
            "intersection_area",
            True,
            True,
            get_true_redistribution(THREE_TRIANGLES_REGION_ID, FOUR_SQUARE_REGION_ID),
            None,
        ),
        (
            "b to a: file still there, ",
            THREE_TRIANGLES_REGION_ID,
            FOUR_SQUARE_REGION_ID,
            "intersection_area",
            None,
            False,
            get_true_redistribution(THREE_TRIANGLES_REGION_ID, FOUR_SQUARE_REGION_ID),
            None,
        ),
        (
            "a to c: no file after not saving, ",
            FOUR_SQUARE_REGION_ID,
            THREE_RECTANGLE_REGION_ID,
            "intersection_area",
            None,
            False,
            None,
            FileNotFoundError,
        ),
        (
            "b to c: using simplified, ",
            THREE_TRIANGLES_REGION_ID,
            THREE_RECTANGLE_REGION_ID,
            "intersection_area",
            False,
            True,
            get_true_redistribution(THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID),
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
