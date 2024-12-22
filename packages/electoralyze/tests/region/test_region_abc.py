import os
import timeit

import geopandas as gpd
import polars as pl
import pytest
from electoralyze import region
from electoralyze.common.geometry import to_geopandas
from electoralyze.common.testing.region_fixture import RegionMocked, read_true_geometry, read_true_metadata
from geopandas import testing as gpd_testing  # noqa: F401
from polars import testing as pl_testing  # noqa: F401


def test_true_region_id_and_name():
    """Check the region id and name structuring works."""
    assert region.SA1_2021.id == "SA1_2021", "Bad region id."
    assert region.SA2_2021.id == "SA2_2021", "Bad region id."
    assert region.SA1_2021.name == "SA1_2021_name", "Bad region name."
    assert region.SA2_2021.name == "SA2_2021_name", "Bad region name."


def test_true_region_file_names():
    """Check the file pathsways work as expected.

    if this fails, try running `region.SA1_2021.process_raw()` locally.
    """
    assert region.SA1_2021.geometry_file.endswith("data/regions/SA1_2021/geometry.parquet"), "Bad region geom file path"
    assert region.SA1_2021.metadata_file.endswith(
        "data/regions/SA1_2021/metadata.parquet"
    ), "Bad region metadata file path"
    assert region.SA1_2021.raw_geometry_file.endswith(
        "data/raw/SA1_2021_AUST_GDA2020.zip"
    ), "Bad region raw geom file path"

    assert os.path.isfile(region.SA1_2021.geometry_file), "Cant find SA1_2021 processed geom."
    assert os.path.isfile(region.SA1_2021.metadata_file), "Cant find SA1_2021 processed metadata."


def test_region_geometry_caches():
    """Test loading caches is snappy."""

    def read_geometries():
        """Read some geometries."""
        region.SA1_2021.geometry  # noqa: B018
        region.SA2_2021.geometry  # noqa: B018

    read_geometries()  # Initially loads cache

    execution_time = timeit.timeit(read_geometries, number=10)
    assert execution_time < 1e-3, "Caching geometries didnt work."


def test_region_fixture_import(region: RegionMocked):
    """Test region fixture imports."""
    gpd.testing.assert_geodataframe_equal(
        region.RegionA.get_raw_geometry().pipe(to_geopandas),
        read_true_geometry("region_a", raw=True).pipe(to_geopandas),
    )
    gpd.testing.assert_geodataframe_equal(
        region.RegionB.get_raw_geometry().pipe(to_geopandas),
        read_true_geometry("region_b", raw=True).pipe(to_geopandas),
    )
    pl.testing.assert_frame_equal(region.RegionA.get_raw_metadata(), read_true_metadata("region_a"))
    pl.testing.assert_frame_equal(region.RegionB.get_raw_metadata(), read_true_metadata("region_b"))


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

    gpd.testing.assert_geodataframe_equal(
        region.RegionA.geometry.pipe(to_geopandas), read_true_geometry("region_a").pipe(to_geopandas)
    )
    gpd.testing.assert_geodataframe_equal(
        region.RegionB.geometry.pipe(to_geopandas), read_true_geometry("region_b").pipe(to_geopandas)
    )

    pl.testing.assert_frame_equal(region.RegionA.metadata, read_true_metadata("region_a"))
    pl.testing.assert_frame_equal(region.RegionB.metadata, read_true_metadata("region_b"))


def test_region_fixture_still_processed(region: RegionMocked):
    """Test region fixture keeps saved data."""
    gpd.testing.assert_geodataframe_equal(
        region.RegionA.geometry.pipe(to_geopandas), read_true_geometry("region_a").pipe(to_geopandas)
    )
    gpd.testing.assert_geodataframe_equal(
        region.RegionB.geometry.pipe(to_geopandas), read_true_geometry("region_b").pipe(to_geopandas)
    )

    pl.testing.assert_frame_equal(region.RegionA.metadata, read_true_metadata("region_a"))
    pl.testing.assert_frame_equal(region.RegionB.metadata, read_true_metadata("region_b"))
