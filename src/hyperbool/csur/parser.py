import json
import os
import xml
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Iterable, Dict

from dataclasses_json import dataclass_json
from lxml import objectify
from lxml.etree import ElementTextIterator
from tqdm import tqdm


@dataclass_json
@dataclass
class StudyCharacteristics:
    study_id: str
    methods: str
    population: str
    interventions: str
    outcomes: str
    notes: str

    def pprint(self) -> str:
        return f"---- {self.study_id} ----\n[P] {self.population}\n[I] {self.interventions}\n[O] {self.outcomes}\n[NOTES] {self.notes}\n[METHODS] {self.methods}"


@dataclass_json
@dataclass
class Study:
    study_id: str
    study_type: str
    author_list: List[str]
    title: str
    year: str
    pmid: str = None

    @property
    def has_pmid(self):
        return self.pmid is not None


@dataclass_json
@dataclass
class CochraneReview:
    id: str
    title: str
    type: str
    summary: str
    abstract: str
    included_studies: List[Study]
    included_study_characteristics: List[StudyCharacteristics]
    appendices: Dict[str, str]


class CochraneReviewParser:
    def __init__(self, fname: Path):
        with open(fname, "rb") as f:
            self.review: xml.etree.ElementTree = objectify.parse(f).getroot()

    def __itertext__(self, el, join_str=" ") -> str:
        if isinstance(el, ElementTextIterator):
            return join_str.join(text for text in el)
        return join_str.join(el.itertext() if el is not isinstance(el, str) else "")

    def serialise(self) -> CochraneReview:
        return CochraneReview(
            id=self.id,
            title=self.title,
            type=self.type,
            summary=self.summary,
            abstract=self.abstract,
            included_studies=[x.to_dict() for x in self.included_studies],
            included_study_characteristics=[x.to_dict() for x in self.included_studies_characteristics],
            appendices=self.appendices
        )

    @property
    def title(self) -> str:
        if self.review.MAIN_TEXT.find("SUMMARY") is None:
            return self.__itertext__(self.review.COVER_SHEET.TITLE)
        return str(self.review.MAIN_TEXT.SUMMARY.TITLE)

    @property
    def id(self) -> str:
        return self.review.get("ID")

    @property
    def type(self) -> str:
        return self.review.get("TYPE")

    @property
    def summary(self) -> str:
        if self.review.MAIN_TEXT.find("SUMMARY") is None:
            return ""
        return self.__itertext__(self.review.MAIN_TEXT.SUMMARY.SUMMARY_BODY)

    @property
    def abstract(self) -> str:
        return self.__itertext__(self.review.MAIN_TEXT.ABSTRACT.itertext())

    @property
    def included_studies_characteristics(self) -> List[StudyCharacteristics]:
        if self.review.find("CHARACTERISTICS_OF_STUDIES") is None:
            return []
        for study_el in self.review.CHARACTERISTICS_OF_STUDIES.CHARACTERISTICS_OF_INCLUDED_STUDIES.iterfind("INCLUDED_CHAR"):
            yield StudyCharacteristics(study_id=study_el.get("STUDY_ID"),
                                       methods=self.__itertext__(study_el.CHAR_METHODS) if study_el.find("CHAR_METHODS") is not None else None,
                                       population=self.__itertext__(study_el.CHAR_PARTICIPANTS) if study_el.find("CHAR_PARTICIPANTS") is not None else None,
                                       interventions=self.__itertext__(study_el.CHAR_INTERVENTIONS) if study_el.find("CHAR_INTERVENTIONS") is not None else None,
                                       outcomes=self.__itertext__(study_el.CHAR_OUTCOMES) if study_el.find("CHAR_OUTCOMES") is not None else None,
                                       notes=self.__itertext__(study_el.CHAR_NOTES) if study_el.find("CHAR_NOTES") is not None else None)

    @property
    def included_studies(self) -> List[Study]:
        if self.review.find("STUDIES_AND_REFERENCES") is None:
            return []
        for study_el in self.review.STUDIES_AND_REFERENCES.STUDIES.INCLUDED_STUDIES.getchildren():
            if study_el.find("REFERENCE") is None:
                continue
            pmid = None
            if study_el.REFERENCE.find("IDENTIFIERS") is not None:
                for identifier in study_el.REFERENCE.IDENTIFIERS.getchildren():
                    if identifier.get("TYPE") == "PUBMED":
                        pmid = identifier.get("VALUE").strip()
                        break
            yield Study(study_id=study_el.get("ID"),
                        study_type=study_el.REFERENCE.get("TYPE"),
                        author_list=[au.strip() for au in self.__itertext__(study_el.REFERENCE.AU).split(",")] if study_el.REFERENCE.find("AU") is not None else None,
                        title=self.__itertext__(study_el.REFERENCE.TI) if study_el.REFERENCE.find("TI") is not None else None,
                        year=self.__itertext__(study_el.REFERENCE.YR) if study_el.REFERENCE.find("YR") is not None else None,
                        pmid=pmid)

    @property
    def population(self) -> List[str]:
        return [s.population for s in self.included_studies_characteristics]

    @property
    def interventions(self) -> List[str]:
        return [s.interventions for s in self.included_studies_characteristics]

    @property
    def outcomes(self) -> List[str]:
        return [s.outcomes for s in self.included_studies_characteristics]

    @property
    def appendices(self) -> Dict[str, Any]:
        if self.review.find("APPENDICES") is None:
            return {}
        return dict([(self.__itertext__(el.TITLE), self.__itertext__(el.APPENDIX_BODY, join_str="\n")) for el in self.review.APPENDICES.findall("APPENDIX")])


def read_folder(folder: Path) -> Iterable[CochraneReview]:
    valid_files = [f for f in os.listdir(str(folder)) if not f.startswith(".")]
    progress = tqdm(valid_files, desc="folder progress", total=len(valid_files), position=0)
    for file in progress:
        progress.postfix = file
        yield CochraneReviewParser(folder / file).serialise()
