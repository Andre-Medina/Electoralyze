import polars as pl
import pytest
from electoralyze.common.testing.region_fixture import (
    FOUR_SQUARE_REGION_ID,
    LEFT_RIGHT_REGION_ID,
    ONE_SQUARE_REGION_ID,
    THREE_TRIANGLES_REGION_ID,
    RegionMocked,
)
from electoralyze.region.redistribute.redistribute import redistribute
from polars import testing  # noqa: F401
from polars.exceptions import ColumnNotFoundError


@pytest.mark.parametrize(
    "_name, region_ids, redistribute_kwargs, redistributed_expected",
    [
        (
            "one square -> quadrants -> L and R, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_via=FOUR_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", "data": 100},
                    ]
                ),
                mapping="intersection_area",
                redistribute_with_full=True,
            ),
            dict(
                data_by_to=pl.DataFrame(
                    [
                        {FOUR_SQUARE_REGION_ID: "N", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "M", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "O", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "P", "data": 25.0},
                    ]
                ),
                data_by_via=pl.DataFrame(
                    [
                        {LEFT_RIGHT_REGION_ID: "L", "data": 50.0},
                        {LEFT_RIGHT_REGION_ID: "R", "data": 50.0},
                    ]
                ),
            ),
        ),
        (
            "Split data, one square -> quadrants -> L and R, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_via=FOUR_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", "data": 70.0},
                        {ONE_SQUARE_REGION_ID: "main", "data": 30.0},
                    ]
                ),
                mapping="intersection_area",
                redistribute_with_full=True,
            ),
            dict(
                data_by_to=pl.DataFrame(
                    [
                        {FOUR_SQUARE_REGION_ID: "N", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "M", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "O", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "P", "data": 25.0},
                    ]
                ),
                data_by_via=pl.DataFrame(
                    [
                        {LEFT_RIGHT_REGION_ID: "L", "data": 50.0},
                        {LEFT_RIGHT_REGION_ID: "R", "data": 50.0},
                    ]
                ),
            ),
        ),
        (
            "one square -> triangles -> L and R, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_via=THREE_TRIANGLES_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", "data": 100},
                    ]
                ),
                mapping="intersection_area",
                redistribute_with_full=True,
            ),
            dict(
                data_by_to=pl.DataFrame(
                    [
                        {THREE_TRIANGLES_REGION_ID: "A", "data": 50.0},
                        {THREE_TRIANGLES_REGION_ID: "B", "data": 25.0},
                        {THREE_TRIANGLES_REGION_ID: "C", "data": 25.0},
                    ]
                ),
                data_by_via=pl.DataFrame(
                    [
                        {LEFT_RIGHT_REGION_ID: "L", "data": 37.5},
                        {LEFT_RIGHT_REGION_ID: "R", "data": 37.5},
                        {LEFT_RIGHT_REGION_ID: None, "data": 25.0},
                    ]
                ),
            ),
        ),
        (
            "L R -> Triangle -> quadrants, ",
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
                redistribute_with_full=True,
            ),
            dict(
                data_by_via=pl.DataFrame(
                    [
                        {FOUR_SQUARE_REGION_ID: "N", "data": 18.0},
                        {FOUR_SQUARE_REGION_ID: "M", "data": 12.0},
                        {FOUR_SQUARE_REGION_ID: "O", "data": 20.0},
                        {FOUR_SQUARE_REGION_ID: "P", "data": 22.0},
                        {FOUR_SQUARE_REGION_ID: None, "data": 24.0},
                    ]
                ),
                data_by_to=pl.DataFrame(
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
        redistributed_expected["data_by_to"],
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
        redistributed_expected["data_by_via"],
        check_row_order=False,
        check_column_order=False,
    )


@pytest.mark.parametrize(
    "_name, region_ids, redistribute_kwargs, redistributed_expected",
    [
        (
            "Mapping as basic dataframe, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=FOUR_SQUARE_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", "data": 100},
                    ]
                ),
                mapping=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: "M", "mapping": 25.0},
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: "N", "mapping": 25.0},
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: "O", "mapping": 25.0},
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: "P", "mapping": 25.0},
                    ]
                ),
                redistribute_with_full=None,
            ),
            dict(
                data_by_to=pl.DataFrame(
                    [
                        {FOUR_SQUARE_REGION_ID: "M", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "N", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "O", "data": 25.0},
                        {FOUR_SQUARE_REGION_ID: "P", "data": 25.0},
                    ]
                ),
            ),
        ),
        (
            "Mapping as less basic dataframe, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=FOUR_SQUARE_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", "data": 100},
                    ]
                ),
                mapping=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: "M", "mapping": 1.0},
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: "N", "mapping": 2.0},
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: "O", "mapping": 3.0},
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: "P", "mapping": 3.0},
                        {ONE_SQUARE_REGION_ID: "main", FOUR_SQUARE_REGION_ID: None, "mapping": 1.0},
                    ]
                ),
                redistribute_with_full=None,
            ),
            dict(
                data_by_to=pl.DataFrame(
                    [
                        {FOUR_SQUARE_REGION_ID: "M", "data": 10.0},
                        {FOUR_SQUARE_REGION_ID: "N", "data": 20.0},
                        {FOUR_SQUARE_REGION_ID: "O", "data": 30.0},
                        {FOUR_SQUARE_REGION_ID: "P", "data": 30.0},
                        {FOUR_SQUARE_REGION_ID: None, "data": 10.0},
                    ]
                ),
            ),
        ),
    ],
)
def test_redistribute_special_mapping(
    region: RegionMocked, _name: str, region_ids: dict, redistribute_kwargs: dict, redistributed_expected: dict
):
    """Test redistribute function works as intended, testing both `from -> to` as well as `from -> via -> to`."""
    redistributed_to = redistribute(
        region_from=region.from_id(region_ids["region_id_from"]),
        region_via=region.from_id(region_ids["region_id_via"]) if "region_id_via" in region_ids else None,
        region_to=region.from_id(region_ids["region_id_to"]),
        **redistribute_kwargs,
    )

    pl.testing.assert_frame_equal(
        redistributed_to,
        redistributed_expected["data_by_to"],
        check_row_order=False,
        check_column_order=False,
    )


@pytest.mark.parametrize(
    "_name, region_ids, redistribute_kwargs, redistributed_expected",
    [
        (
            "data_by_from has wrong ids, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "O", "data": 100}]),
                mapping="intersection_area",
                redistribute_with_full=True,
            ),
            dict(
                errors=ValueError,
            ),
        ),
        (
            "data_by_from has wrong ids: want warnings no errors, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "O", "data": 100.0}]),
                mapping="intersection_area",
                redistribute_with_full=True,
                errors="warning",
            ),
            dict(
                data_by_to=pl.DataFrame(pl.DataFrame(schema={LEFT_RIGHT_REGION_ID: pl.String, "data": pl.Float64})),
            ),
        ),
        (
            "Same from and to region ids raises error, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=ONE_SQUARE_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "O", "data": 100.0}]),
            ),
            dict(
                errors=ValueError,
            ),
        ),
        (
            "`data_by_from` doesn't have region_if_from raises error,  ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{"nothing": "O", "data": 100.0}]),
            ),
            dict(
                errors=ValueError,
            ),
        ),
        (
            "no exiting mapping raises error,  ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "O", "data": 100.0}]),
                mapping="intersection_area",
                redistribute_with_full=None,
            ),
            dict(
                errors=FileNotFoundError,
            ),
        ),
        (
            "no data columns raises error, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "O"}]),
                mapping="intersection_area",
                redistribute_with_full=None,
            ),
            dict(
                errors=ValueError,
            ),
        ),
        (
            "Custom mapping is missing a region column, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "main", "data": 100.0}]),
                mapping=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", "mapping": 1.0},
                    ]
                ),
            ),
            dict(
                errors=ColumnNotFoundError,
            ),
        ),
        (
            "Custom mapping is missing a mapping column, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "main", "data": 100.0}]),
                mapping=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", LEFT_RIGHT_REGION_ID: "M"},
                    ]
                ),
            ),
            dict(
                errors=ColumnNotFoundError,
            ),
        ),
        (
            "Custom mapping is fine, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "main", "data": 100.0}]),
                mapping=pl.DataFrame(
                    [
                        {ONE_SQUARE_REGION_ID: "main", LEFT_RIGHT_REGION_ID: "M", "mapping": 1.0},
                    ]
                ),
            ),
            dict(
                data_by_to=pl.DataFrame([{LEFT_RIGHT_REGION_ID: "M", "data": 100.0}]),
            ),
        ),
        (
            "Not implemented aggregation method, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "main", "data": 100.0}]),
                redistribute_with_full=True,
                aggregation="min",
            ),
            dict(
                errors=NotImplementedError,
            ),
        ),
        (
            "Not implemented centroid distance, ",
            dict(
                region_id_from=ONE_SQUARE_REGION_ID,
                region_id_to=LEFT_RIGHT_REGION_ID,
            ),
            dict(
                data_by_from=pl.DataFrame([{ONE_SQUARE_REGION_ID: "main", "data": 100.0}]),
                redistribute_with_full=True,
                mapping="centroid_distance",
            ),
            dict(
                errors=NotImplementedError,
            ),
        ),
    ],
)
def test_redistribute_error_conditions(
    region: RegionMocked, _name: str, region_ids: dict, redistribute_kwargs: dict, redistributed_expected: dict
):
    """Test redistribute function works as intended, testing both `from -> to` as well as `from -> via -> to`."""
    if redistributed_expected.get("errors") is None:
        redistributed_via = redistribute(
            region_from=region.from_id(region_ids["region_id_from"]),
            region_via=region.from_id(region_ids["region_id_via"]) if "region_id_via" in region_ids else None,
            region_to=region.from_id(region_ids["region_id_to"]),
            **redistribute_kwargs,
        )

        pl.testing.assert_frame_equal(
            redistributed_via,
            redistributed_expected["data_by_to"],
            check_row_order=False,
            check_column_order=False,
        )

    else:
        with pytest.raises(redistributed_expected["errors"]):
            redistribute(
                region_from=region.from_id(region_ids["region_id_from"]),
                region_via=region.from_id(region_ids["region_id_via"]) if "region_id_via" in region_ids else None,
                region_to=region.from_id(region_ids["region_id_to"]),
                **redistribute_kwargs,
            )
