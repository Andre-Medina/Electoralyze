import os
from enum import Enum

COMMON_DIR: str = os.path.dirname(os.path.realpath(__file__))
ELECTORALYZE_DIR: str = os.path.join(COMMON_DIR, "..")
ROOT_DIR: str = os.path.join(ELECTORALYZE_DIR, "../../..")

REGION_SIMPLIFY_TOLERANCE: float = 0.0001

COORDINATE_REFERENCE_SYSTEM: int = 4326

# Proj.db for pyogrio
PROD_DB_FILE = os.path.join(ROOT_DIR, ".pixi/envs/default/lib/python3.13/site-packages/pyogrio/proj_data/")
os.environ["PROJ_LIB"] = PROD_DB_FILE
os.environ["GDAL_DATA"] = PROD_DB_FILE


####### Election Enums ########


class Preference(str, Enum):
    """Possible preferences for an election.

    - `PRIMARY` - Primary vote
    - `FOUR_CANDIDATE` - Four candidate preference (4CP)
    - `THREE_CANDIDATE` - Three candidate preference (3CP)
    - `TWO_CANDIDATE` - Two candidate preference (2CP)

    """

    PRIMARY = "primary_vote"
    FOUR_CANDIDATE = "four_candidate"
    THREE_CANDIDATE = "three_candidate"
    TWO_CANDIDATE = "two_candidate"


class Chamber(str, Enum):
    """Possible chambers for an election.

    - `PRESIDENT` - Not technically a chamber, but segment of the election.
    - `UPPER` - For Bicameral legislatures, the 'upper' house (typically senate)
    - `MIDDLE` - For Tricameral legislatures
    - `LOWER` - For Bicameral legislatures, the 'lower' house (typically house of representatives)
    - `HOUSE` - For Unicameral legislatures

    """

    PRESIDENT = "president"
    UPPER = "upper"
    MIDDLE = "middle"
    LOWER = "lower"
    HOUSE = "house"


class VoteBreakdown(str, Enum):
    """Possible vote breakdowns for an election.

    - `PARTY` - Breakdown by party
    - `DEFACTO_PARTY` - Breakdown by "defacto" party
    - `CANDIDATE` - Breakdown by candidate
    - `POLITICAL_ALIGNMENT` - Breakdown by political alignment

    """

    PARTY = "party"
    DEFACTO_PARTY = "defacto_party"
    CANDIDATE = "candidate"
    POLITICAL_ALIGNMENT = "political_alignment"
