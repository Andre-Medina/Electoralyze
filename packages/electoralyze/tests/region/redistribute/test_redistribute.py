import polars as pl
import pytest
from electoralyze.common.testing.region_fixture import (
    FOUR_SQUARE_REGION_ID,
    LEFT_RIGHT_REGION_ID,
    THREE_TRIANGLES_REGION_ID,
    RegionMocked,
)
from electoralyze.region.redistribute.redistribute import redistribute
from polars import testing  # noqa: F401


@pytest.mark.parametrize(
    "_name, region_ids, redistribute_kwargs, redistributed_expected",
    [
        (
            "from L R -> via Triangle -> to quadrants, ",
            dict(
                region_id_from=LEFT_RIGHT_REGION_ID,
                region_id_to=FOUR_SQUARE_REGION_ID,
                region_id_via=THREE_TRIANGLES_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame(
                    [
                        {LEFT_RIGHT_REGION_ID: "L", "data": 32},
                        {LEFT_RIGHT_REGION_ID: "R", "data": 64},
                    ]
                ),
                mapping="intersection_area",
                redistribute_with_full=False,
            ),
            dict(
                via=pl.DataFrame(
                    [
                        {FOUR_SQUARE_REGION_ID: "N", "data": 18.0},
                        {FOUR_SQUARE_REGION_ID: "M", "data": 12.0},
                        {FOUR_SQUARE_REGION_ID: "O", "data": 20.0},
                        {FOUR_SQUARE_REGION_ID: "P", "data": 22.0},
                        {FOUR_SQUARE_REGION_ID: None, "data": 24.0},
                    ]
                ),
                to=pl.DataFrame(
                    [
                        {THREE_TRIANGLES_REGION_ID: "A", "data": 48.0},
                        {THREE_TRIANGLES_REGION_ID: "B", "data": 16.0},
                        {THREE_TRIANGLES_REGION_ID: "C", "data": 32.0},
                    ]
                ),
            ),
        ),
    ],
)
def test_redistribute_via(
    region: RegionMocked, _name: str, region_ids: dict, redistribute_kwargs: dict, redistributed_expected: dict
):
    """Test redistribute function works as intended, testing both `from -> to` as well as `from -> via -> to`."""
    redistributed_to = redistribute(
        region_from=region.from_id(region_ids["region_id_from"]),
        region_to=region.from_id(region_ids["region_id_via"]),
        **redistribute_kwargs,
    )

    pl.testing.assert_frame_equal(
        redistributed_to,
        redistributed_expected["to"],
        check_row_order=False,
        check_column_order=False,
    )

    redistributed_via = redistribute(
        region_from=region.from_id(region_ids["region_id_from"]),
        region_via=region.from_id(region_ids["region_id_via"]),
        region_to=region.from_id(region_ids["region_id_to"]),
        **redistribute_kwargs,
    )

    pl.testing.assert_frame_equal(
        redistributed_via,
        redistributed_expected["via"],
        check_row_order=False,
        check_column_order=False,
    )
