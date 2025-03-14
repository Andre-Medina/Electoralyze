import os
import tempfile
import time
from random import choices

import polars as pl
import pytest
from electoralyze.common.metric import Metric, MetricRegion
from electoralyze.common.testing.region_fixture import (
    ONE_SQUARE_REGION_ID,
    REGION_JSONS,
    THREE_RECTANGLE_REGION_ID,
    THREE_TRIANGLES_REGION_ID,
    RegionMocked,
)
from electoralyze.region.region_abc import RegionABC

## FIXTURES AND FUNCTIONS


def _process_raw_test(
    parent_metric: Metric, region: RegionABC, force_new: bool, download: bool, **_kwargs: dict
) -> pl.DataFrame:
    """Fake `process_raw` function for any metric or region."""
    if region.id not in [THREE_RECTANGLE_REGION_ID, ONE_SQUARE_REGION_ID]:
        raise NotImplementedError("This region is not implemented.")

    region_ids = choices(REGION_JSONS[region.id][region.id], k=4)  # noqa: S311
    schema = pl.Schema(
        {
            "region_id": pl.String,
            parent_metric.category_column: pl.Int32,
            parent_metric.value_column: pl.Float32,
        }
    )

    if download:
        data = pl.DataFrame(
            {
                "region_id": region_ids,
                parent_metric.category_column: [2020, 2021] * 2,
                parent_metric.value_column: [-10.0, -20.0, -30.0, -40.0],
            },
            schema=schema,
        )

    else:
        data = pl.DataFrame(
            {
                "region_id": region_ids,
                parent_metric.category_column: [2020, 2021] * 2,
                parent_metric.value_column: [10.0, 20.0, 30.0, 40.0],
            },
            schema=schema,
        )

    return data


def _process_raw_population(
    parent_metric: Metric, region: RegionABC, force_new: bool, download: bool, **_kwargs: dict
) -> pl.DataFrame:
    """Fake `process_raw` function for any metric or region."""
    data = pl.DataFrame(
        {"region_id": ["A", "A", "B", "B"], "year": [2020, 2021] * 2, "population": [10.0, 20.0, 30.0, 40.0]}
    )
    return data


def _process_raw_test_extra_kwargs(
    parent_metric: Metric, region: RegionABC, extra: str, force_new: bool, download: bool, **_kwargs: dict
) -> pl.DataFrame:
    """Fake `process_raw` function for any metric or region."""
    data = pl.DataFrame(
        {
            region.id: ["A", "A", "B", "B"],
            parent_metric.category_column: [2020, 2021] * 2,
            parent_metric.value_column: [10.0, 20.0, 30.0, 40.0],
        }
    )

    return data


def _process_raw_no_return(
    parent_metric: Metric, region: RegionABC, force_new: bool, download: bool, **_kwargs: dict
) -> None:
    """Empty `process_raw` function for mocking."""
    pass


def _process_raw_no_parent(region: RegionABC, force_new: bool, download: bool, **_kwargs: dict) -> pl.DataFrame:
    """Empty `process_raw` function for mocking."""
    pass


def _process_raw_no_parent_type(
    parent_metric, region: RegionABC, force_new: bool, download: bool, **_kwargs: dict
) -> pl.DataFrame:
    """Empty `process_raw` function for mocking."""
    pass


### METRIC REGION TESTS


def test_metric_region_create(region: RegionMocked):
    """Test creating a metric region works as intended."""
    metric_region_a = MetricRegion(region=region.rectangle, process_raw=_process_raw_population)
    assert metric_region_a.is_primary is True
    metric_region_b = MetricRegion(region=region.triangle, redistribute_from=region.rectangle)
    assert metric_region_b.is_primary is False
    metric_region_c = MetricRegion(
        region=region.triangle, redistribute_from=region.rectangle, redistribute_kwargs={"region_via": region.quadrant}
    )
    assert metric_region_c.is_primary is False

    with pytest.raises(ValueError):
        MetricRegion(region=region.rectangle)
    with pytest.raises(ValueError):
        MetricRegion(region=region.rectangle, redistribute_kwargs={"region_via": region.quadrant})


@pytest.mark.parametrize(
    "_name, inputs, error",
    [
        (
            "Primary region: typical working, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "process_raw": _process_raw_test,
            },
            None,
        ),
        (
            "Primary region: extra working, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "process_raw": _process_raw_test_extra_kwargs,
                "process_raw_kwargs": {"extra": "helloworld"},
            },
            None,
        ),
        (
            "Primary region: passed redistribute from, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "process_raw": _process_raw_test,
                "redistribute_from": THREE_TRIANGLES_REGION_ID,
            },
            ValueError,
        ),
        (
            "Primary region: passed redistribute kwargs, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "process_raw": _process_raw_test,
                "redistribute_kwargs": {"aggregation": "mean"},
            },
            ValueError,
        ),
        (
            "Primary region: missing extra, ",
            {"region": THREE_TRIANGLES_REGION_ID, "process_raw": _process_raw_test_extra_kwargs},
            ValueError,
        ),
        (
            "Primary region: unexpected extras, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "process_raw": _process_raw_test,
                "process_raw_kwargs": {"extra": "helloworld"},
            },
            ValueError,
        ),
        (
            "Primary region: bad type kwargs, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "process_raw": _process_raw_test_extra_kwargs,
                "process_raw_kwargs": {"extra": 123},
            },
            ValueError,
        ),
        (
            "Primary region: no return type, ",
            {"region": THREE_TRIANGLES_REGION_ID, "process_raw": _process_raw_no_return},
            ValueError,
        ),
        (
            "Primary region: no parent kwarg, ",
            {"region": THREE_TRIANGLES_REGION_ID, "process_raw": _process_raw_no_parent},
            ValueError,
        ),
        (
            "Primary region: no parent type, ",
            {"region": THREE_TRIANGLES_REGION_ID, "process_raw": _process_raw_no_parent_type},
            ValueError,
        ),
        (
            "Secondary region: typical working, ",
            {"region": THREE_TRIANGLES_REGION_ID, "redistribute_from": THREE_RECTANGLE_REGION_ID},
            None,
        ),
        (
            "Secondary region: extra working, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "redistribute_from": THREE_RECTANGLE_REGION_ID,
                "redistribute_kwargs": {"aggregation": "mean"},
            },
            None,
        ),
        (
            "Secondary region: process raw kwargs, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "redistribute_from": THREE_RECTANGLE_REGION_ID,
                "process_raw_kwargs": {"a": 1},
            },
            ValueError,
        ),
        ("Secondary region: missing from, ", {"region": THREE_TRIANGLES_REGION_ID}, ValueError),
        (
            "Secondary region: bad kwargs, ",
            {
                "region": THREE_TRIANGLES_REGION_ID,
                "redistribute_from": THREE_RECTANGLE_REGION_ID,
                "redistribute_kwargs": {"aggregation_": "mean"},
            },
            ValueError,
        ),
    ],
)
def test_bad_metric_region_create(_name: str, inputs: dict, error: type[Exception], region: RegionMocked):
    """Test creating a bad metric region raises an error where needed."""
    inputs["region"] = region.from_id(inputs["region"])
    if "redistribute_from" in inputs:
        inputs["redistribute_from"] = region.from_id(inputs["redistribute_from"])

    if error is not None:
        with pytest.raises(error):
            MetricRegion(**inputs)
    else:
        MetricRegion(**inputs)


# Basic Metric


@pytest.fixture
def basic_metric_fixture(region: RegionMocked):
    """Create a basic metric for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        my_metric = Metric(
            name="population",
            processed_path=f"{temp_dir}/data/my_metric/{{region_id}}.parquet",
            allowed_regions=[
                MetricRegion(region=region.rectangle, process_raw=_process_raw_test),
                MetricRegion(region=region.triangle, redistribute_from=region.rectangle),
            ],
        )
        yield region, my_metric, temp_dir


def test_basic_metric_create(basic_metric_fixture: tuple[RegionMocked, Metric, str]):
    """Test creating a metric works as intended."""
    (
        _region,
        my_metric,
        _temp_dir,
    ) = basic_metric_fixture
    assert my_metric is not None, "Did not create basic_metric"


def test_basic_metric_process_raw(basic_metric_fixture: tuple[RegionMocked, Metric, str]):
    """Test creating a metric works as intended."""
    (
        region,
        my_metric,
        _temp_dir,
    ) = basic_metric_fixture

    with pytest.raises(FileNotFoundError):
        my_metric.by(region.rectangle)

    my_metric.process_raw()
    my_metric.by(region.rectangle)

    with pytest.raises(NotImplementedError):
        my_metric.by(region.triangle)

    with pytest.raises(KeyError):
        my_metric.by(region.quadrant)


def test_basic_metric_basic_processed_path(basic_metric_fixture: tuple[RegionMocked, Metric, str]):
    """Test creating a metric works as intended."""
    (
        region,
        my_metric,
        temp_dir,
    ) = basic_metric_fixture
    my_metric.process_raw()

    processed_path = my_metric.get_processed_path().format(region_id=region.rectangle.id)
    assert processed_path == f"{temp_dir}/data/my_metric/{region.rectangle.id}.parquet", "bad path formatting."

    assert os.path.exists(processed_path), "The processed path does not exist."


def test_basic_metric_force_new(basic_metric_fixture: tuple[RegionMocked, Metric, str]):
    """Test creating a metric works as intended."""
    (
        region,
        my_metric,
        _temp_dir,
    ) = basic_metric_fixture
    my_metric.process_raw()
    processed_path = my_metric.get_processed_path().format(region_id=region.rectangle.id)

    time_initial = os.path.getmtime(processed_path)

    my_metric.process_raw(force_new=False)
    time_redownload = os.path.getmtime(processed_path)
    assert time_initial == time_redownload, "The data should not have changed."

    time.sleep(1e-4)
    my_metric.process_raw(force_new=True)
    time_force_new = os.path.getmtime(processed_path)
    assert time_initial != time_force_new, "The data should have changed."


def test_basic_metric_download(basic_metric_fixture: tuple[RegionMocked, Metric, str]):
    """Test creating a metric works as intended."""
    (
        region,
        my_metric,
        _temp_dir,
    ) = basic_metric_fixture

    my_metric.process_raw(download=True)
    data_with_download_in_region = my_metric.by(region.rectangle)
    assert (data_with_download_in_region["value"] > 0).any() is False, "Download went wrong, All values meant to be <0."

    my_metric.process_raw(download=False)
    data_with_download_in_region = my_metric.by(region.rectangle)
    assert (
        data_with_download_in_region["value"] < 0
    ).any() is True, "No Download went wrong, All values meant to be >0."


@pytest.mark.parametrize(
    "_name, input_override, error",
    [
        ("base case works, ", {}, None),
        ("Bad data type value, ", {"data_type": "BAD CATEGORY"}, ValueError),
        ("Bad category column type, ", {"category_column": 123}, ValueError),
        ("Bad value column type, ", {"value_column": 123}, ValueError),
        ("Bad processed path type, ", {"processed_path": 123}, ValueError),
        ("Bad allowed regions type, ", {"allowed_regions": 123}, ValueError),
        ("schema getter not a pl.Schema, ", {"schema": 123}, ValueError),
        ("Category column not in schema, ", {"category_column": "no_year"}, ValueError),
        ("Path has no {region_id}, ", {"processed_path": "not_a_folder/data/my_metric/region_id.parquet"}, ValueError),
        ("Path is not parquet, ", {"processed_path": "not_a_folder/data/my_metric/{region_id}.csv"}, ValueError),
        ("Value column not in schema, ", {"category_column": "no_year"}, ValueError),
        ("Schema has bad cols, ", {"schema": pl.Schema({"region_id": pl.String})}, ValueError),
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
        processed_path="not_a_folder/data/my_metric/{region_id}.parquet",
        allowed_regions=[MetricRegion(region=region.rectangle, process_raw=_process_raw_test)],
    )
    if error is not None:
        with pytest.raises(error):
            Metric(**(good_data | input_override))
    else:
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
                MetricRegion(region=region.quadrant, process_raw=_process_raw_test),
                MetricRegion(region=region.rectangle, redistribute_from=region.triangle),
            ],
        )


# Subclassing

SUB = "sub"
POPULATION_NAME = "population"
AGE_NAME = "population"


@pytest.fixture
def sub_metric_fixture(region: RegionMocked) -> tuple[RegionMocked, Metric, Metric, str]:
    """Create a sub metric for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:

        class SubMetric(Metric):
            name_suffix: str = SUB
            processed_path: None = None
            category_column: str = "year"
            data_type: str = "numeric"
            schema_getter: None = None

            def get_processed_path(self) -> str:
                file = f"{temp_dir}/data/{{sub_metric}}/{{metric}}/{{region_id}}.parquet".format(
                    sub_metric=self.name_suffix,
                    metric=self.name,
                    region_id="{region_id}",
                )
                return file

            def _get_schema(self, region: RegionABC) -> pl.Schema:  # noqa: ARG002
                """Overridable function to change kwargs in schema getter."""
                schema = pl.Schema(
                    {"region_id": pl.String, self.category_column: pl.Int32, self.value_column: pl.Float32}
                )
                return schema

        population_metric = SubMetric(
            name=POPULATION_NAME,
            value_column=POPULATION_NAME,
            allowed_regions=[
                MetricRegion(region=region.rectangle, process_raw=_process_raw_test),
                MetricRegion(region=region.triangle, redistribute_from=region.rectangle),
            ],
        )
        age_metric = SubMetric(
            name=AGE_NAME,
            value_column=AGE_NAME,
            allowed_regions=[
                MetricRegion(region=region.square, process_raw=_process_raw_test),
                MetricRegion(region=region.l_and_r, redistribute_from=region.square),
            ],
        )

        return region, population_metric, age_metric, temp_dir


def test_subclassing_metric_create(sub_metric_fixture: tuple[RegionMocked, Metric, Metric, str]):
    """Test creating a sub metric works as intended."""
    _region, population_metric, age_metric, _temp_dir = sub_metric_fixture
    assert population_metric is not None, "Did not create population metric"
    assert age_metric is not None, "Did not create age metric"


def test_subclassing_metric_process_raw(sub_metric_fixture: tuple[RegionMocked, Metric, Metric, str]):
    """Test creating a sub metric works as intended."""
    region, population_metric, age_metric, _temp_dir = sub_metric_fixture

    with pytest.raises(FileNotFoundError):
        population_metric.by(region.rectangle)
    population_metric.process_raw()
    population_metric.by(region.rectangle)

    with pytest.raises(NotImplementedError):
        population_metric.by(region.triangle)
    with pytest.raises(NotImplementedError):
        age_metric.by(region.l_and_r)

    with pytest.raises(FileNotFoundError):
        age_metric.by(region.square)
    age_metric.process_raw()
    age_metric.by(region.square)

    with pytest.raises(KeyError):
        population_metric.by(region.square)
    with pytest.raises(KeyError):
        age_metric.by(region.rectangle)


def test_subclassing_metric_path(sub_metric_fixture: tuple[RegionMocked, Metric, Metric, str]):
    """Test creating a sub metric works as intended."""
    region, population_metric, age_metric, temp_dir = sub_metric_fixture

    processed_path = population_metric.get_processed_path().format(region_id=region.triangle.id)
    assert (
        processed_path == f"{temp_dir}/data/{SUB}/{POPULATION_NAME}/{region.triangle.id}.parquet"
    ), "bad path formatting."
    processed_path = age_metric.get_processed_path().format(region_id=region.triangle.id)
    assert processed_path == f"{temp_dir}/data/{SUB}/{AGE_NAME}/{region.triangle.id}.parquet", "bad path formatting."
