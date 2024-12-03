import tempfile

import geopandas as gpd
import polars as pl
import pytest
from electoralyze import region
from electoralyze.common.geometry import to_geopandas
from electoralyze.region.region_abc import RegionABC
from geopandas import testing as gpd_testing  # noqa: F401
from polars import testing as pl_testing  # noqa: F401


@pytest.mark.parametrize(
    "region_class",
    [
        (region.SA1_2021),
        (region.SA2_2021),
    ],
)
def test_region_process_raw(region_class: RegionABC):
    """Test recreating region geometry and compares it to whats stored."""
    # assert os.path.isfile(region.SA1_2021.raw_geometry_file), "Cant find SA1_2021 raw data." Issue #10

    with tempfile.TemporaryDirectory(delete=False) as temp_dir:
        #### Getting existing data ####

        original_geometry_file = region_class.geometry_file
        original_metadata_file = region_class.metadata_file

        geometry_original = region_class.geometry
        metadata_original = region_class.metadata

        region_class.cache_clear()

        #### Making temp folders #####

        region_class.geometry_file = f"{temp_dir}/geometry/{region_class.id}.parquet"
        region_class.metadata_file = f"{temp_dir}/metadata/{region_class.id}.parquet"

        #### Re processing data ####

        region_class.process_raw()

        geometry_new = region_class.geometry
        metadata_new = region_class.metadata

        #### Comparing ####

        gpd.testing.assert_geodataframe_equal(
            geometry_new.sort(region_class.id).pipe(to_geopandas),
            geometry_original.sort(region_class.id).pipe(to_geopandas),
            check_less_precise=True,
        )
        pl.testing.assert_frame_equal(metadata_new.sort(region_class.id), metadata_original.sort(region_class.id))

        #### Resetting ####

        region_class.geometry_file = original_geometry_file
        region_class.metadata_file = original_metadata_file
