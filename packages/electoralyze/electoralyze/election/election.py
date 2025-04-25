from pydantic import BaseModel

from electoralyze.common.constants import Chamber, Preference, VoteBreakdown
from electoralyze.common.metric import Metric


class ElectionChamber(BaseModel):
    """..."""

    def results(preference: Preference, vote_breakdown: VoteBreakdown) -> Metric:
        """...."""
        pass


class ElectionPeriod(BaseModel):
    """..."""

    chambers: dict[Chamber, ElectionChamber]

    def for_(self, chamber: Chamber, /) -> ElectionChamber:
        """Get a specific election chamber."""
        election_chamber = self.chambers[chamber]
        return election_chamber
