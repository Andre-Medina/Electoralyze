import os

COMMON_DIR: str = os.path.dirname(os.path.realpath(__file__))
ELECTORALYZE_DIR: str = os.path.join(COMMON_DIR, "..")
ROOT_DIR: str = os.path.join(ELECTORALYZE_DIR, "../../..")

REGION_SIMPLIFY_TOLERANCE: float = 0.0001

COORDINATE_REFERENCE_SYSTEM: int = 4326
