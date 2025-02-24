import tempfile

import polars as pl
import pytest
from electoralyze.common.metric import Metric, MetricRegion
from electoralyze.common.testing.region_fixture import (
    THREE_RECTANGLE_REGION_ID,
    THREE_TRIANGLES_REGION_ID,
    RegionMocked,
)


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
    with tempfile.TemporaryDirectory() as temp_dir:
        my_metric = Metric(
            name="population",
            data_type="numeric",
            category_column="year",
            value_column="population",
            schema_getter=lambda region: pl.Schema({region.id: pl.String, "year": pl.Int64, "population": pl.Float64}),
            processed_path=f"{temp_dir}/data/my_metric/{{region_id}}.parquet",
            allowed_regions=[
                MetricRegion(region=region.rectangle, process_raw=_process_raw_population),
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

        processed_path = my_metric.get_processed_path().format(region_id=region.triangle.id)
        assert processed_path == f"{temp_dir}/data/my_metric/{region.triangle.id}.parquet", "bad path formatting."

    # Check path is formatted correctly


@pytest.mark.parametrize(
    "_name, input_override, error",
    [
        ("Bad data type value, ", {"data_type": "BAD CATEGORY"}, ValueError),
        ("Bad category column type, ", {"category_column": 123}, ValueError),
        ("Bad value column type, ", {"value_column": 123}, ValueError),
        ("Bad processed path type, ", {"processed_path": 123}, ValueError),
        ("Bad allowed regions type, ", {"allowed_regions": 123}, ValueError),
        ("schema getter not a function, ", {"schema_getter": 123}, ValueError),
        ("Category column not in schema, ", {"category_column": "no_year"}, ValueError),
        ("Path has no {region_id}, ", {"processed_path": "not_a_folder/data/my_metric/region_id.parquet"}, ValueError),
        ("Path is not parquet, ", {"processed_path": "not_a_folder/data/my_metric/{region_id}.csv"}, ValueError),
        ("Value column not in schema, ", {"category_column": "no_year"}, ValueError),
        ("Schema has bad cols, ", {"schema_getter": lambda region: pl.Schema({region.id: pl.String})}, ValueError),
        ("No regions, ", {"allowed_regions": []}, ValueError),
        ("No real regions, ", {"allowed_regions": [1, 2]}, ValueError),
        (
            "No regions with data getters, ",
            {
                "allowed_regions_as_dict": {
                    "region": THREE_TRIANGLES_REGION_ID,
                    "redistribute_from": THREE_RECTANGLE_REGION_ID,
                }
            },
            ValueError,
        ),
    ],
)
def test_bad_metric_creations(_name: str, input_override: dict, error: type[Exception], region: RegionMocked):
    """Test creating a bad metric errors as intended."""
    if "allowed_regions_as_dict" in input_override:
        input_override = input_override | {
            "allowed_regions": MetricRegion(
                region=region.from_id(input_override["allowed_regions_as_dict"].get("region")),
                redistribute_from=region.from_id(input_override["allowed_regions_as_dict"].get("redistribute_from")),
            )
        }
        input_override.pop("allowed_regions_as_dict")

    good_data = dict(
        name="population",
        data_type="numeric",
        category_column="year",
        value_column="population",
        schema_getter=lambda region: pl.Schema({region.id: pl.String, "year": pl.Int64, "population": pl.Float64}),
        processed_path="not_a_folder/data/my_metric/{region_id}.parquet",
        allowed_regions=[MetricRegion(region=region.rectangle, process_raw=_process_raw_population)],
    )
    # Bad data type
    with pytest.raises(error):
        Metric(**(good_data | input_override))


def test_bad_metric_creations_bad_region_getter(region: RegionMocked):
    """Test creating a bad metric errors as intended."""
    with pytest.raises(ValueError):
        Metric(
            name="population",
            data_type="numeric",
            category_column="year",
            value_column="population",
            schema_getter=lambda region: pl.Schema({region.id: pl.String, "year": pl.Int64, "population": pl.Float64}),
            processed_path="not_a_folder/data/my_metric/{region_id}.parquet",
            allowed_regions=[
                MetricRegion(region=region.quadrant, process_raw=_process_raw_population),
                MetricRegion(region=region.rectangle, redistribute_from=region.triangle),
            ],
        )


def test_subclassing_metric():
    """Test creating a sub metric works as intended."""
    # Create a submetric
    # Create several metrics
    # Check path is formatted correctly
    pass


def _process_raw_population(**_kwargs) -> pl.DataFrame:
    """Fake `process_raw` function for the population."""
    data = pl.DataFrame(
        {
            THREE_RECTANGLE_REGION_ID: ["A", "A", "B", "B"],
            "year": [2020, 2021] * 2,
            "population": [10.0, 20.0, 30.0, 40.0],
        }
    )

    return data
