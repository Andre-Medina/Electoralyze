import tempfile

import polars as pl
import pytest
from electoralyze.common.metric import Metric, MetricRegion
from electoralyze.common.testing.region_fixture import RegionMocked


def test_metric_basic_create(region: RegionMocked):
    """Test creating a metric works as intended."""

    def process_raw_population(**_kwargs) -> pl.DataFrame:
        data = pl.DataFrame(
            {"SA1_2021": ["A", "A", "B", "B"], "year": [2020, 2021] * 2, "population": [10.0, 20.0, 30.0, 40.0]}
        )

        return data

    with tempfile.TemporaryDirectory() as temp_dir:
        my_metric = Metric(
            name="population",
            file=f"{temp_dir}/data/my_metric/{{region}}.parquet",
            data_type="categorical",
            category_column="year",
            data_column="population",
            allowed_regions=[
                MetricRegion(region=region.RegionA, process_raw=process_raw_population),
                MetricRegion(region=region.RegionB, redistribute_from=region.RegionA),
            ],
            schema=lambda region: pl.Schema({region.id: pl.String, "year": pl.Int64, "population": pl.Float64}),
        )

        with pytest.raises(FileNotFoundError):
            my_metric.by(region.RegionA)
        my_metric.process_raw()
        my_metric.by(region.RegionA)

        with pytest.raises(NotImplementedError):
            my_metric.by(region.RegionB)
