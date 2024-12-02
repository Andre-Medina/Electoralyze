import os

import polars as pl
import polars_st as st

from ..region_abc import RegionABC
from .utils import MAPPING_OPTIONS


def get_region_mapping_base(
    region_from: RegionABC,
    region_to: RegionABC,
    *,
    mapping: MAPPING_OPTIONS,
    redistribute_with_full: bool | None = None,
) -> pl.DataFrame:
    """Get region mapping base.

    Returns
    -------
    pl.DataFrame, mapping from region_from to region_to.
    ```python

    ```

    """
    mapping_file = _get_region_mapping_file(region_from, region_to, mapping=mapping)

    if not os.path.exists(mapping_file):
        if redistribute_with_full is None:
            raise FileNotFoundError(
                f"Mapping file not found for `{region_from.id}` -> `{region_to.id}` under mapping `{mapping}`. "
                "Consider generating it with `create_region_mapping_base` or pass `redistribute_with_full=True/False`."
            )
        region_mapping = create_region_mapping_base(
            region_from,
            region_to,
            mapping=mapping,
            redistribute_with_full=redistribute_with_full,
            save_data=False,
        )
    else:
        region_mapping = pl.read_parquet(mapping_file)

    return region_mapping


def create_region_mapping_base(
    region_from: RegionABC,
    region_to: RegionABC,
    *,
    mapping: MAPPING_OPTIONS,
    redistribute_with_full: bool = True,
    save_data: bool = True,
):
    """Create region mapping base.

    WIP.
    """
    if redistribute_with_full:
        geometry_from: st.GeoDataFrame = region_from.get_raw_geometry()
        geometry_to: st.GeoDataFrame = region_from.get_raw_geometry()
    else:
        geometry_from: st.GeoDataFrame = region_from.geometry
        geometry_to: st.GeoDataFrame = region_to.geometry

    match mapping:
        case "intersection_area":
            region_mapping = _create_intersection_area_mapping(geometry_from, geometry_to)
        case "centroid_distance":
            region_mapping = _create_centroid_distance_mapping(geometry_from, geometry_to)

    if save_data:
        mapping_file = _get_region_mapping_file(region_from, region_to, mapping=mapping)
        os.makedirs(mapping_file.rsplit("/", maxsplit=1)[0], exist_ok=True)
        region_mapping.write_parquet(mapping_file)

    return region_mapping


def _create_intersection_area_mapping(
    geometry_from: st.GeoDataFrame,
    geometry_to: st.GeoDataFrame,
) -> pl.DataFrame:
    """Create mapping from one region to another based on intersection area."""
    geometry_combined = geometry_from.rename({"geometry": "geometry_from"}).join(
        geometry_to.rename({"geometry": "geometry_to"}), how="cross"
    )
    intersection_area = geometry_combined.select(
        pl.exclude("geometry_from", "geometry_to"),
        st.geom("geometry_from").st.intersection(st.geom("geometry_to")).st.area().alias("intersection_area"),
    ).filter(pl.col("intersection_area") != 0)

    return intersection_area


def _get_remaining_area(
    region_id: str,
    geometry: st.GeoDataFrame,
    intersection_area: pl.DataFrame,
) -> pl.DataFrame:
    """Find remaining area which hasnt been assigned to another region."""
    # alt_region_id
    pass


def _create_centroid_distance_mapping(
    geometry_from: st.GeoDataFrame,
    geometry_to: st.GeoDataFrame,
) -> pl.DataFrame:
    """Create mapping from one region to another based on centroid distance."""
    raise NotImplementedError("This method is not implemented yet.")


def _get_region_mapping_file(
    region_from: RegionABC,
    region_to: RegionABC,
    *,
    mapping: MAPPING_OPTIONS,
) -> str:
    """Returns the path to the mapping file for the given region."""
    regions = list({region_from.id, region_to.id})
    mapping_file = region_from._redistribute_file.format(
        mapping=mapping,
        region_a=regions[0],
        region_b=regions[1],
    )
    return mapping_file
