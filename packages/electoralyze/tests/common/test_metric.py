import tempfile

import polars as pl
import pytest
from electoralyze.common.metric import Metric, MetricRegion
from electoralyze.common.testing.region_fixture import RegionMocked


def test_metric_region_create(region: RegionMocked):
    """Test creating a metric region works as intended."""

    def process_raw_population(**_kwargs) -> pl.DataFrame:
        data = pl.DataFrame(
            {"SA1_2021": ["A", "A", "B", "B"], "year": [2020, 2021] * 2, "population": [10.0, 20.0, 30.0, 40.0]}
        )
        return data

    MetricRegion(region=region.rectangle, process_raw=process_raw_population)
    MetricRegion(region=region.triangle, redistribute_from=region.rectangle)
    MetricRegion(region=region.triangle, redistribute_from=region.rectangle, redistribute_kwargs={"a": 1})

    with pytest.raises(ValueError):
        MetricRegion(region=region.rectangle)
    with pytest.raises(ValueError):
        MetricRegion(region=region.rectangle, redistribute_kwargs={"a": 1})

    # Bad process_raw function
    # Bad process_raw function kwarg list (cant accept `parent_metric`)
    # Bad redistribute from


def test_metric_basic_create(region: RegionMocked):
    """Test creating a metric works as intended."""

    def process_raw_population(**_kwargs) -> pl.DataFrame:
        data = pl.DataFrame(
            {
                region.rectangle.id: ["A", "A", "B", "B"],
                "year": [2020, 2021] * 2,
                "population": [10.0, 20.0, 30.0, 40.0],
            }
        )

        return data

    with tempfile.TemporaryDirectory() as temp_dir:
        my_metric = Metric(
            name="population",
            data_type="numeric",
            category_column="year",
            data_column="population",
            schema_getter=lambda region: pl.Schema({region.id: pl.String, "year": pl.Int64, "population": pl.Float64}),
            processed_path=f"{temp_dir}/data/my_metric/{{region_id}}.parquet",
            allowed_regions=[
                MetricRegion(region=region.rectangle, process_raw=process_raw_population),
                MetricRegion(region=region.triangle, redistribute_from=region.rectangle),
            ],
        )

        with pytest.raises(FileNotFoundError):
            my_metric.by(region.rectangle)
        my_metric.process_raw()
        my_metric.by(region.rectangle)

        with pytest.raises(NotImplementedError):
            my_metric.by(region.triangle)

        with pytest.raises(KeyError):
            my_metric.by(region.quadrant)

    # Check path is formatted correctly


def test_bad_metric_creations(region: RegionMocked):
    """Test creating a bad metric errors as intended."""
    # Bad data type
    # Bad other inputs arent string
    # Data getter isnt a function
    # Not passing key parameters
    # Allowed regions is empty

    pass


def test_subclassing_metric():
    """Test creating a sub metric works as intended."""
    # Create a submetric
    # Create several metrics
    # Check path is formatted correctly
    pass
