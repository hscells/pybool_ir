from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dataclasses_json import dataclass_json
from lxml import objectify


class CochraneReview():
    def __init__(self, fname: Path):
        with open(fname, "rb") as f:
            self.review = objectify.parse(f).getroot()

    def __itertext__(self, el, join_str=" ") -> str:
        return join_str.join(el.itertext() if el is not isinstance(el, str) else "")

    @property
    def title(self) -> str:
        return self.review.MAIN_TEXT.SUMMARY.TITLE

    @property
    def id(self) -> str:
        return self.review.get("ID")

    @property
    def type(self) -> str:
        return self.review.get("TYPE")

    @property
    def summary(self) -> str:
        return self.__itertext__(self.review.MAIN_TEXT.SUMMARY.SUMMARY_BODY)

    @property
    def abstract(self) -> str:
        return self.__itertext__(self.review.MAIN_TEXT.ABSTRACT.itertext())

    @property
    def population(self) -> str:
        return self.__itertext__(self.review.MAIN_TEXT.BODY.METHODS.SELECTION_CRITERIA.CRIT_PARTICIPANTS)

    @property
    def interventions(self) -> str:
        return self.__itertext__(self.review.MAIN_TEXT.BODY.METHODS.SELECTION_CRITERIA.CRIT_INTERVENTIONS)

    @property
    def outcomes(self) -> str:
        return self.__itertext__(self.review.MAIN_TEXT.BODY.METHODS.SELECTION_CRITERIA.CRIT_OUTCOMES.CRIT_OUTCOMES_PRIMARY)

    @property
    def appendices(self) -> str:
        return [(self.__itertext__(el.TITLE), self.__itertext__(el.APPENDIX_BODY, join_str="\n")) for el in self.review.APPENDICES.findall("APPENDIX")]
