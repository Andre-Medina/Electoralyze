# import polars as pl
# import pytest
# from electoralyze.common.testing.region_fixture import (
#     FOUR_SQUARE_REGION_ID,
#     LEFT_RIGHT_REGION_ID,
#     ONE_SQUARE_REGION_ID,
#     THREE_TRIANGLES_REGION_ID,
#     RegionMocked,
# )
# from electoralyze.region.redistribute.redistribute import redistribute
# from polars import testing  # noqa: F401
# from polars.exceptions import ColumnNotFoundError


# @pytest.mark.parametrize(
#     "_name, region_ids, redistribute_kwargs, redistributed_expected",
#     [
#         (
