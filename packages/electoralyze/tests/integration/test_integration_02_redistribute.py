import tempfile

import polars as pl
import pytest
from electoralyze import region
from electoralyze.region.redistribute.mapping import MAPPING_OPTIONS, get_region_mapping_base

# from electoralyze.region.redistribute.redistribute import redistribute
from electoralyze.region.region_abc import RegionABC
from polars import testing  # noqa: F401


@pytest.mark.parametrize(
    "region_from, region_to, mapping_method",
    [
        (region.Federal2022, region.SA1_2021, "intersection_area"),
        (region.SA2_2021, region.SA1_2021, "intersection_area"),
    ],
)
def test_get_region_mapping_base(region_from: RegionABC, region_to: RegionABC, mapping_method: MAPPING_OPTIONS):
    """Test recreating region mapping and compares it to that stored."""
    with tempfile.TemporaryDirectory(delete=False) as temp_dir:
        #### Getting existing data ####

        mapping_original = get_region_mapping_base(
            region_from=region_from,
            region_to=region_to,
            mapping_method=mapping_method,
            redistribute_with_full=None,
            save_data=False,
            force_new=False,
        )

        root_dir_original = region_from._root_dir

        region_from.cache_clear()
        region_to.cache_clear()

        #### Making temp folders #####

        region_from._root_dir = temp_dir
        region_to._root_dir = temp_dir

        #### Re processing data ####

        mapping_new_generated = get_region_mapping_base(
            region_from=region_from,
            region_to=region_to,
            mapping_method=mapping_method,
            save_data=True,
            force_new=True,
            redistribute_with_full=True,
        )

        mapping_new_stored = get_region_mapping_base(
            region_from=region_from,
            region_to=region_to,
            mapping_method=mapping_method,
            save_data=False,
            force_new=False,
            redistribute_with_full=None,
        )

        #### Comparing ####

        pl.testing.assert_frame_equal(
            mapping_new_generated,
            mapping_new_stored,
            check_row_order=False,
            check_column_order=False,
        )

        pl.testing.assert_frame_equal(
            mapping_original,
            mapping_new_stored,
            check_row_order=False,
            check_column_order=False,
        )

        #### Resetting ####

        region_from._root_dir = root_dir_original
        region_to._root_dir = root_dir_original
