from typing import Literal

from .region_abc import RegionABC as __RegionABC
from .regions.SA1_2021 import SA1_2021
from .regions.SA2_2021 import SA2_2021

__all__ = ["SA1_2021", "SA2_2021"]
ALL_IDS = __all__
REGION_IDS_T = Literal["SA1_2021", "SA2_2021"]


def from_id(region_id: REGION_IDS_T, /) -> __RegionABC:
    """Get region given a regions id."""
    if region_id not in __all__:
        raise ValueError(f"Unknown region: {region_id!r}")

    region_ = globals().get(region_id)
    return region_
