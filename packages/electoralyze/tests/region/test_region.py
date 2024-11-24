import os

import pytest
from electoralyze import region
from electoralyze.common.testing.region_fixture import RegionMocked


def test_true_region_file_names():
    """Check the file pathsways work as expected.

    if this fails, try running `region.SA1_2021.process_raw()` locally.
    """
    assert region.SA1_2021.geometry_file.endswith("region/regions/SA1_2021/geometry/SA1_2021.parquet")
    assert region.SA1_2021.metadata_file.endswith("region/regions/SA1_2021/metadata.parquet")
    assert region.SA1_2021.raw_geometry_file.endswith("data/raw/ASGA/2021/SA1/SA1_2021_AUST_GDA2020.shp")

    assert os.path.isfile(region.SA1_2021.raw_geometry_file)
    assert os.path.isfile(region.SA1_2021.geometry_file)
    assert os.path.isfile(region.SA1_2021.metadata_file)


def test_region_fixture_import(region: RegionMocked):
    """Test region fixture imports."""
    region.RegionA.get_raw_geometry()
    region.RegionA.get_raw_metadata()


def test_region_fixture_process(region: RegionMocked):
    """Test region fixture processes raw data and saves it."""
    with pytest.raises(FileNotFoundError):
        region.RegionA.geometry  # noqa:B018
    with pytest.raises(FileNotFoundError):
        region.RegionA.metadata  # noqa:B018
    with pytest.raises(FileNotFoundError):
        region.RegionB.geometry  # noqa:B018
    with pytest.raises(FileNotFoundError):
        region.RegionB.metadata  # noqa:B018

    region.RegionA.process_raw()
    region.RegionB.process_raw()

    region.RegionA.geometry  # noqa:B018
    region.RegionA.metadata  # noqa:B018
    region.RegionB.geometry  # noqa:B018
    region.RegionB.metadata  # noqa:B018


def test_region_fixture_still_processed(region: RegionMocked):
    """Test region fixture keeps saved data."""
    region.RegionA.geometry  # noqa:B018
    region.RegionA.metadata  # noqa:B018
    region.RegionB.geometry  # noqa:B018
    region.RegionB.metadata  # noqa:B018
