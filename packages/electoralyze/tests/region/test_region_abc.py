import os
import tempfile
import timeit

import geopandas as gpd
import polars as pl
import polars_st as st
import pytest
from electoralyze import region
from electoralyze.common.functools import classproperty
from electoralyze.common.geometry import to_geopandas
from electoralyze.common.testing.region_fixture import (
    FOUR_SQUARE_REGION_ID,
    REGION_IDS,
    RegionMocked,
    read_true_geometry,
    read_true_metadata,
)
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
    assert region.SA1_2021.raw_geometry_url.endswith("SA1_2021_AUST_SHP_GDA2020.zip"), "Bad region raw geom url"
    assert region.SA1_2021.raw_geometry_url.startswith("https://www.abs.gov.au"), "Bad region raw geom url"
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
    # Test one manually
    gpd.testing.assert_geodataframe_equal(
        region.quadrant.get_raw_geometry().pipe(to_geopandas),
        read_true_geometry(FOUR_SQUARE_REGION_ID, raw=True).pipe(to_geopandas),
    )
    pl.testing.assert_frame_equal(region.quadrant.get_raw_metadata(), read_true_metadata(FOUR_SQUARE_REGION_ID))

    # Test the rest through a loop
    for region_id in REGION_IDS:
        gpd.testing.assert_geodataframe_equal(
            region.from_id(region_id).get_raw_geometry().pipe(to_geopandas),
            read_true_geometry(region_id, raw=True).pipe(to_geopandas),
        )
        pl.testing.assert_frame_equal(region.from_id(region_id).get_raw_metadata(), read_true_metadata(region_id))


def test_region_fixture_process(region: RegionMocked):
    """Test region fixture processes raw data and saves it."""
    region.remove_processed_files()

    # Test one manually
    with pytest.raises(FileNotFoundError):
        region.quadrant.geometry  # noqa:B018
    with pytest.raises(FileNotFoundError):
        region.quadrant.metadata  # noqa:B018
    region.quadrant.process_raw()
    gpd.testing.assert_geodataframe_equal(
        region.quadrant.geometry.pipe(to_geopandas), read_true_geometry(FOUR_SQUARE_REGION_ID).pipe(to_geopandas)
    )
    pl.testing.assert_frame_equal(region.quadrant.metadata, read_true_metadata(FOUR_SQUARE_REGION_ID))

    # Test the rest through a loop
    for region_id in REGION_IDS:
        region.from_id(region_id).remove_processed_files()
        with pytest.raises(FileNotFoundError):
            region.from_id(region_id).geometry  # noqa:B018
        with pytest.raises(FileNotFoundError):
            region.from_id(region_id).metadata  # noqa:B018

        region.from_id(region_id).process_raw()

        gpd.testing.assert_geodataframe_equal(
            region.from_id(region_id).geometry.pipe(to_geopandas), read_true_geometry(region_id).pipe(to_geopandas)
        )
        pl.testing.assert_frame_equal(region.from_id(region_id).metadata, read_true_metadata(region_id))


def test_region_fixture_still_processed(region: RegionMocked):
    """Test region fixture keeps saved data."""
    # Test one manually
    gpd.testing.assert_geodataframe_equal(
        region.quadrant.geometry.pipe(to_geopandas), read_true_geometry(FOUR_SQUARE_REGION_ID).pipe(to_geopandas)
    )
    pl.testing.assert_frame_equal(region.quadrant.metadata, read_true_metadata(FOUR_SQUARE_REGION_ID))

    # Test the rest through a loop
    for region_id in REGION_IDS:
        gpd.testing.assert_geodataframe_equal(
            region.from_id(region_id).geometry.pipe(to_geopandas), read_true_geometry(region_id).pipe(to_geopandas)
        )
        pl.testing.assert_frame_equal(region.from_id(region_id).metadata, read_true_metadata(region_id))


def test_region_downloads_raw(region: RegionMocked):
    """Test region downloads raw data when needed."""
    with tempfile.TemporaryDirectory() as temp_dir:
        raw_geometry_file = f"{temp_dir}/raw_geometry/country_code/shape.shp"

        class TempRegion(region._RegionMockedABC):
            raw_geometry_url = (
                "https://raw.githubusercontent.com/datasets/" "geo-boundaries-world-110m/master/countries.geojson"
            )

            @classproperty
            def raw_geometry_file(self) -> str:
                """Raw file."""
                return raw_geometry_file

            @classproperty
            def id(self) -> str:
                """Id for country_code."""
                return "country_code"

            @classmethod
            def _transform_geometry_raw(cls, geometry_raw: st.GeoDataFrame) -> st.GeoDataFrame:
                """Transformed file."""
                geometry_with_metadata = geometry_raw.select(
                    pl.col("sov_a3").alias("country_code"),
                    pl.struct(
                        pl.col("name_long").alias("name"),
                    ).alias("metadata"),
                    pl.col("geometry"),
                )
                return geometry_with_metadata

        ### Testing through `download_data` ###

        assert not os.path.exists(TempRegion.raw_geometry_file), "The should not be data yet."

        TempRegion.download_data()
        assert os.path.exists(TempRegion.raw_geometry_file), "Data should have downloaded."

        time_initial = os.path.getmtime(TempRegion.raw_geometry_file)

        TempRegion.download_data(force_new=False)
        time_redownload = os.path.getmtime(TempRegion.raw_geometry_file)
        assert time_initial == time_redownload, "The data should not have changed."

        TempRegion.download_data(force_new=True)
        time_force_new = os.path.getmtime(TempRegion.raw_geometry_file)
        assert time_initial != time_force_new, "The data should have changed."

        ### Reset data ###
        TempRegion._geometry_cached.cache_clear()
        TempRegion._metadata_cached.cache_clear()
        os.remove(TempRegion.raw_geometry_file)
        assert not os.path.exists(TempRegion.raw_geometry_file), "The should be no data anymore."

        ### Testing through `process_raw`` ###

        with pytest.raises(FileNotFoundError):
            TempRegion.process_raw(download=False)

        TempRegion.process_raw()  # download=True
        assert os.path.exists(TempRegion.raw_geometry_file), "Data should have downloaded."

        time_initial = os.path.getmtime(TempRegion.raw_geometry_file)

        TempRegion.process_raw()  # download=True
        time_redownload = os.path.getmtime(TempRegion.raw_geometry_file)
        assert time_initial == time_redownload, "The data should not have changed."

        TempRegion.process_raw(force_new=True)
        time_force_new = os.path.getmtime(TempRegion.raw_geometry_file)
        assert time_initial != time_force_new, "The data should have changed."
