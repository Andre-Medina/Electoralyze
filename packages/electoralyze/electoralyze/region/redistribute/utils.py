from typing import Literal

WEIGHT_OPTIONS = Literal["population"]
MAPPING_OPTIONS = Literal["intersection_area", "centroid_distance"]
AGGREGATION_OPTIONS = Literal["sum", "mean", "count", "max", "min"]
