from typing import Literal

import polars as pl

from .region_abc import RegionABC

#### REDISTRIBUTE ###########
MAPPING_METHODS = ["intersection_area", "centroid_distance"]

WEIGHT_OPTIONS = Literal["population"]
MAPPING_OPTIONS = Literal["intersection_area", "centroid_distance"]
AGGREGATION_OPTIONS = Literal["sum", "mean", "count", "max", "min"]


def redistribute(
    data_by_from: pl.DataFrame,
    *,
    index_columns: list[str] | None,
    region_from: RegionABC,
    region_to: RegionABC,
    region_through: RegionABC | None = None,
    weights: WEIGHT_OPTIONS | None = None,
    mapping: MAPPING_OPTIONS | pl.DataFrame = "intersection_area",
    aggregation: AGGREGATION_OPTIONS = "sum",
    redistribute_with_full: bool | None = None,
) -> pl.DataFrame:
    """Redistribute data from one region to another.

    Parameters
    ----------
    data_by_from: pl.DataFrame,
    index_columns: list[str] | None, Index columns in the input dataframe to keep.
    region_from: RegionABC, From region to redistribute, should be a column in the dataframe.
    region_to: RegionABC, To region to redistribute, Will output data with this column.
    region_through: RegionABC | None, If given, will convert region_from -> region_through -> region_to. Useful to
    weights: Literal["population"] | None, weighting to use to redistribute data.
        - None: redistribute by pure `mapping` as the weight.
        - "population", Will use population as a weight.
    mapping: Literal["intersection_area", "centroid_distance"] | pl.DataFrame, mapping to
        geometrically redistribute data.
        - "intersection_area": Will use the intersection area of each regions.
        - "centroid_distance": Will use the centroid distance of each regions.
        - pl.DataFrame: Will use the custom mapping, with columns `region_from`, `region_to`, and `value`.
    aggregation: Literal["sum", "mean", "count", "max", "min"], mapping to aggregate the redistributed data.
        - "sum": Will take the proportional sum the sub regions.
        - "mean": Will take the proportional mean of sub regions.
        - "count": Will take the proportional count of sub regions.
        - "max": Will take the absolute max of sub regions.
        - "min": Will take the absolute min of sub regions.
    redistribute_with_full: bool | None = None,
        - None = only use existing static redistribution map files.
        - False = Will create redistribution maps using simplified geometries for each region.
        - True = Will create redistribution maps using full geometries for each region (EXPENSIVE!).
    """
    if region_through:
        data_by_through = redistribute(
            data_by_from,
            index_columns=index_columns,
            region_from=region_from,
            region_to=region_through,
            weights=weights,
            mapping=mapping,
            aggregation=aggregation,
            redistribute_with_full=redistribute_with_full,
        )
        data_by_from = data_by_through
        region_from = region_through

    region_map = _get_region_map(region_from, region_to, mapping, weights)
    data_distributed = _distribute(data_by_from, region_from, region_map)
    data_by_from = _aggregate(data_distributed, region_from, region_to, aggregation, index_columns)

    return data_by_from


def _get_region_map(
    region_from: RegionABC,
    region_to: RegionABC,
    mapping: MAPPING_OPTIONS | pl.DataFrame,
    weights: WEIGHT_OPTIONS | None,
) -> pl.DataFrame:
    pass


def _distribute(data_by_from, region_from, region_map) -> pl.DataFrame:
    """Distributes mapping over regions."""
    pass


def _combine():
    """Combines mapping and distributed data."""
    pass


def _aggregate(
    data_distributed: pl.DataFrame,
    region_from: RegionABC,
    region_to: RegionABC,
    aggregation: AGGREGATION_OPTIONS,
    index_columns: list[str] | None,
) -> pl.DataFrame:
    """Aggregates the fully distributed data to the final region."""
    pass
