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
    _create_intersection_area_mapping,
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
    "_name, test_case",
    [
        (
            "a to b: basic no saving, ",
            dict(
                region_id_from=FOUR_SQUARE_REGION_ID,
                region_id_to=THREE_TRIANGLES_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=True,
                save_data=False,
                expected=get_true_redistribution(THREE_TRIANGLES_REGION_ID, FOUR_SQUARE_REGION_ID),
                error=None,
            ),
        ),
        (
            "b to a: file not saved, ",
            dict(
                region_id_from=THREE_TRIANGLES_REGION_ID,
                region_id_to=FOUR_SQUARE_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=None,
                expected=None,
                error=FileNotFoundError,
            ),
        ),
        (
            "a to c: no file at all, ",
            dict(
                region_id_from=FOUR_SQUARE_REGION_ID,
                region_id_to=THREE_RECTANGLE_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=None,
                expected=None,
                error=FileNotFoundError,
            ),
        ),
        (
            "b to c: using simplified, ",
            dict(
                region_id_from=THREE_TRIANGLES_REGION_ID,
                region_id_to=THREE_RECTANGLE_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=False,
                expected=get_true_redistribution(THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID),
                error=None,
            ),
        ),
        (
            "b to c: trying to use centroid, ",
            dict(
                region_id_from=THREE_TRIANGLES_REGION_ID,
                region_id_to=THREE_RECTANGLE_REGION_ID,
                mapping_method="centroid_distance",
                redistribute_with_full=False,
                expected=None,
                error=NotImplementedError,
            ),
        ),
        (
            "b to c: trying to save simplified, ",
            dict(
                region_id_from=THREE_TRIANGLES_REGION_ID,
                region_id_to=THREE_RECTANGLE_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=False,
                save_data=True,
                expected=None,
                error=ValueError,
            ),
        ),
        (
            "a to b, saving basic, ",
            dict(
                region_id_from=FOUR_SQUARE_REGION_ID,
                region_id_to=THREE_TRIANGLES_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=True,
                save_data=True,
                expected=get_true_redistribution(THREE_TRIANGLES_REGION_ID, FOUR_SQUARE_REGION_ID),
                error=None,
            ),
        ),
        (
            "b to a: file still exists, ",
            dict(
                region_id_from=THREE_TRIANGLES_REGION_ID,
                region_id_to=FOUR_SQUARE_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=None,
                save_data=False,
                expected=get_true_redistribution(THREE_TRIANGLES_REGION_ID, FOUR_SQUARE_REGION_ID),
                error=None,
            ),
        ),
        (
            "a to c: no file after not saving, ",
            dict(
                region_id_from=FOUR_SQUARE_REGION_ID,
                region_id_to=THREE_RECTANGLE_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=None,
                save_data=False,
                expected=None,
                error=FileNotFoundError,
            ),
        ),
        (
            "b to c: using simplified, ",
            dict(
                region_id_from=THREE_TRIANGLES_REGION_ID,
                region_id_to=THREE_RECTANGLE_REGION_ID,
                mapping_method="intersection_area",
                redistribute_with_full=False,
                save_data=False,
                expected=get_true_redistribution(THREE_TRIANGLES_REGION_ID, THREE_RECTANGLE_REGION_ID),
                error=None,
            ),
        ),
    ],
)
def test_get_region_mapping_base(
    region: RegionMocked,
    _name: str,
    test_case: dict,
):
    """Test region fixture keeps saved data."""
    region_mapping_kwargs = dict(
        region_from=region.from_id(test_case["region_id_from"]),
        region_to=region.from_id(test_case["region_id_to"]),
        mapping_method=test_case["mapping_method"],
        redistribute_with_full=test_case["redistribute_with_full"],
        save_data=test_case.get("save_data", False),
        force_new=test_case.get("force_new", False),
    )

    if test_case["error"] is None:
        region_mapping_base = get_region_mapping_base(**region_mapping_kwargs)
        pl.testing.assert_frame_equal(
            region_mapping_base,
            test_case["expected"],
            check_column_order=False,
            check_row_order=False,
        )

    else:
        with pytest.raises(test_case["error"]):
            get_region_mapping_base(**region_mapping_kwargs)
