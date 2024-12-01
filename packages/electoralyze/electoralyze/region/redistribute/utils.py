import os
from typing import Literal

from electoralyze.common.constants import ROOT_DIR

WEIGHT_OPTIONS = Literal["population"]
MAPPING_OPTIONS = Literal["intersection_area", "centroid_distance"]
AGGREGATION_OPTIONS = Literal["sum", "mean", "count", "max", "min"]

REDISTRIBUTE_FILE = os.path.join(ROOT_DIR, "data/regions/redistribute/{mapping}/{region_a}_{region_b}.parquet")
