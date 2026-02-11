"""
Off-the-shelf indexer for PubMed articles.
"""

import calendar
import gzip
import os
import glob
import xml.etree.ElementTree as et
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Iterable
from xml.etree.ElementTree import Element

import lucene
from lupyne import engine
from lupyne.engine.documents import Hit
from tqdm.auto import tqdm

from pybool_ir.index.document import Document
from pybool_ir.index.index import Indexer, SearcherMixin

from org.apache.lucene import document

assert lucene.getVMEnv() or lucene.initVM()

DEFAULT_YEAR = 1900
DEFAULT_MONTH = 1
DEFAULT_DAY = 1


class ClinicalTrialsGovArticle(Document):
    """
    This is a special override of the Document class for CTGOV articles. The constructor takes in all the fields that are required for CTGOV articles.
    """

    def __init__(self, org_study_id:str,
                    secondary_id: str,
                    nct_id: str,
                    brief_title: str,
                    official_title: str,
                    sponsors: List[str],
                    source: str,
                    brief_summary: str,
                    detailed_description: str,
                    overall_status: str,
                    phase: str,
                    study_type: str,
                    has_expanded_access: str,
                    primary_outcome: str,
                    secondary_outcomes: str,
                    condition: str,
                    intervention: str,
                    criteria: str,
                    gender: str,
                    minimum_age: int,
                    maximum_age: int,
                    healthy_volunteers: str,
                    location: str,
                    location_countries: List[str],
                    verification_date: datetime,
                    study_first_submitted: datetime,
                    study_first_submitted_qc: datetime,
                    study_first_posted: datetime,
                    last_update_submitted: datetime,
                    last_update_submitted_qc: datetime,
                    last_update_posted: datetime,
                    keyword: str,
                    intervention_browse: List[str],
                    condition_browse: List[str],
                    **optional_fields):
        super().__init__(**{**{
            "id": nct_id,
            "org_study_id": org_study_id,
            "secondary_id": secondary_id,
            "nct_id": nct_id,
            "brief_title": brief_title,
            "official_title": official_title,
            "sponsors": sponsors,
            "source": source,
            "brief_summary": brief_summary,
            "detailed_description": detailed_description,
            "overall_status": overall_status,
            "phase": phase,
            "study_type": study_type,
            "has_expanded_access": has_expanded_access,
            "primary_outcome": primary_outcome,
            "secondary_outcomes": secondary_outcomes,
            "condition": condition,
            "intervention": intervention,
            "criteria": criteria,
            "gender": gender,
            "minimum_age": minimum_age,
            "maximum_age": maximum_age,
            "healthy_volunteers": healthy_volunteers,
            "location": location,
            "location_countries": location_countries,
            "verification_date": verification_date,
            "study_first_submitted": study_first_submitted,
            "study_first_submitted_qc": study_first_submitted_qc,
            "study_first_posted": study_first_posted,
            "last_update_submitted": last_update_submitted,
            "last_update_submitted_qc": last_update_submitted_qc,
            "last_update_posted": last_update_posted,
            "keyword": keyword,
            "intervention_browse": intervention_browse,
            "condition_browse": condition_browse,
        }, **optional_fields})

    @staticmethod
    def from_hit(hit: Hit):
        """
        Create a PubmedArticle from a lucene Hit. This method also removes the `__id__` and `__score__` fields from the hit.
        A document prior to indexing should be equivalent to a document retrieved from a hit using this method.
        """
        d = hit.dict("org_study_id",
                     "secondary_id",
                     "nct_id",
                     "brief_title",
                     "official_title",
                     "sponsors",
                     "source",
                     "brief_summary",
                     "detailed_description",
                     "overall_status",
                     "phase",
                     "study_type",
                     "has_expanded_access",
                     "primary_outcome",
                     "secondary_outcomes",
                     "condition",
                     "intervention",
                     "criteria",
                     "gender",
                     "minimum_age",
                     "maximum_age",
                     "healthy_volunteers",
                     "location",
                     "location_countries",
                     "verification_date",
                     "study_first_submitted",
                     "study_first_submitted_qc",
                     "study_first_posted",
                     "last_update_submitted",
                     "late_update_submitted_qc",
                     "last_update_posted",
                     "keyword",
                     "intervention_browse"
                     "condition_browse")
        del d["__id__"]
        del d["__score__"]
        return ClinicalTrialsGovArticle.from_dict(d)

def parse_ctgov_date(date_str: str) -> datetime:
    """
    Parse a date string from a CTGOV record. The returned value is a tuple of (year, month, day).
    """
    day = DEFAULT_DAY
    month = DEFAULT_MONTH
    year = DEFAULT_YEAR

    date_str = date_str.split(" ")

    month_mapping = {
        "January": 1,
        "February": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12,
    }

    month = month_mapping[date_str[0]]

    if len(date_str) == 2:
        year = int(date_str[1])
    else:
        day = int(date_str[1].replace(",",""))
        year = int(date_str[2])
    return datetime(year=year, month=month, day=day)


def parse_ctgov_duration(age: str) -> int:
    age = age.split(" ")

    dur_mapping = {
        "Days": 1,
        "Months": 30,
        "Years": 365,
    }

    age_t = int(age[0])
    age_d = int(dur_mapping[age[1]])
    return age_t * age_d

def parse_ctgov_article(element: Element) -> ClinicalTrialsGovArticle:
    """
    Parse a PubmedArticle node from a Pubmed XML element.
    """
    org_study_id = "" if element.find("id_info/org_study_id") is None else element.find("id_info/org_study_id").text
    secondary_id = "" if element.find("id_info/secondary_id") is None else element.find("id_info/secondary_id").text
    nct_id = element.find("id_info/nct_id").text
    brief_title = element.find("brief_title").text
    official_title = "" if element.find("official_title") is None else element.find("official_title").text
    sponsors = [el.text for el in element.findall("sponsors/*/source")]
    source = element.find("source").text
    brief_summary = element.find("source").text
    detailed_description = "".join([el.text for el in element.findall("brief_summary/textblock")])
    detailed_description = "".join([el.text for el in element.findall("detailed_description/textblock")])
    overall_status = element.find("overall_status").text
    phase = "" if element.find("phase") is None else element.find("phase").text
    study_type = element.find("study_type").text
    has_expanded_access = "" if element.find("has_expanded_access") is None else element.find("has_expanded_access").text
    pd = "" if element.find("primary_outcome/description") is None else element.find("primary_outcome/description").text
    pt = "" if element.find("primary_outcome/time_frame") is None else element.find("primary_outcome/time_frame").text
    pm = "" if element.find("primary_outcome/measure") is None else element.find("primary_outcome/measure").text
    primary_outcome =  pd + pt + pm
    def so(e, t):
        return "" if e.find(t) is None else e.find(t).text
    secondary_outcomes = "" if element.find("secondary_outcome") is None else [so(el,"measure") + so(el,"time_frame") + so(el,"description") for el in element.findall("secondary_outcome")]
    condition = "" if element.find("condition") is None else element.find("condition").text
    intervention = "" if element.find("intervention/intervention_type") is None else element.find("intervention/intervention_type").text + element.find("intervention/intervention_name").text
    criteria = "".join([el.text for el in element.findall("criteria/textblock")])
    gender = "" if element.find("criteria/gender") is None else element.find("criteria/gender").text
    minimum_age = 0 if element.find("criteria/minimum_age") is None else parse_ctgov_duration(element.find("criteria/minimum_age").text)
    maximum_age = 999 if element.find("criteria/maximum_age") is None else parse_ctgov_duration(element.find("criteria/maximum_age").text)
    healthy_volunteers = "" if element.find("criteria/healthy_volunteers") is None else element.find("criteria/healthy_volunteers").text
    location = [el.text for el in element.findall("location/facility/name")]
    location_countries = [el.text for el in element.findall("location_countries/country")]
    verification_date = datetime(year=DEFAULT_YEAR,month=DEFAULT_MONTH,day=DEFAULT_DAY) if element.find("verification_date") is None else parse_ctgov_date(element.find("verification_date").text)
    study_first_submitted = datetime(year=DEFAULT_YEAR,month=DEFAULT_MONTH,day=DEFAULT_DAY) if element.find("study_first_submitted") is None else parse_ctgov_date(element.find("study_first_submitted").text)
    study_first_submitted_qc = datetime(year=DEFAULT_YEAR,month=DEFAULT_MONTH,day=DEFAULT_DAY) if element.find("study_first_submitted_qc") is None else parse_ctgov_date(element.find("study_first_submitted_qc").text)
    study_first_posted = datetime(year=DEFAULT_YEAR,month=DEFAULT_MONTH,day=DEFAULT_DAY) if element.find("study_first_posted") is None else parse_ctgov_date(element.find("study_first_posted").text)
    last_update_submitted = datetime(year=DEFAULT_YEAR,month=DEFAULT_MONTH,day=DEFAULT_DAY) if element.find("last_update_submitted") is None else parse_ctgov_date(element.find("last_update_submitted").text)
    last_update_submitted_qc = datetime(year=DEFAULT_YEAR,month=DEFAULT_MONTH,day=DEFAULT_DAY) if element.find("last_update_submitted_qc") is None else parse_ctgov_date(element.find("last_update_submitted_qc").text)
    last_update_posted = datetime(year=DEFAULT_YEAR,month=DEFAULT_MONTH,day=DEFAULT_DAY) if element.find("last_update_posted") is None else parse_ctgov_date(element.find("last_update_posted").text)
    keyword = "" if element.find("keyword") is None else element.find("keyword").text
    intervention_browse = [] if element.find("intervention_browse") is not None else [el.text for el in element.findall("intervention_browse/mesh_term")]
    condition_browse =  [] if element.find("condition_browse") is not None else [el.text for el in element.findall("condition_browse/mesh_term")]
    
    return ClinicalTrialsGovArticle(
        org_study_id=org_study_id,
        secondary_id=secondary_id,
        nct_id=nct_id,
        brief_title=brief_title,
        official_title=official_title,
        sponsors=sponsors,
        source=source,
        brief_summary=brief_summary,
        detailed_description=detailed_description,
        overall_status=overall_status,
        phase=phase,
        study_type=study_type,
        has_expanded_access=has_expanded_access,
        primary_outcome=primary_outcome,
        secondary_outcomes=secondary_outcomes,
        condition=condition,
        intervention=intervention,
        criteria=criteria,
        gender=gender,
        minimum_age=minimum_age,
        maximum_age=maximum_age,
        healthy_volunteers=healthy_volunteers,
        location=location,
        location_countries=location_countries,
        verification_date=verification_date,
        study_first_submitted=study_first_submitted,
        study_first_submitted_qc=study_first_submitted_qc,
        study_first_posted=study_first_posted,
        last_update_submitted=last_update_submitted,
        last_update_submitted_qc=last_update_submitted_qc,
        last_update_posted=last_update_posted,
        keyword=keyword,
        intervention_browse=intervention_browse,
        condition_browse=condition_browse,
    )


class ClinicalTrialsGovIndexer(Indexer, SearcherMixin):
    """
    Off-the-shelf indexer for CTGOV XML files.

    >>> from pybool_ir.index.ctgov import ClinicalTrialsGovIndexer
    >>>
    >>> with ClinicalTrialsGovIndexer("path/to/index", store_fields=True) as idx:
    >>> 	idx.bulk_index("path/to/baseline")

    """

    @staticmethod
    def read_file(fname: Path) -> Iterable[Document]:
        """
        Read a single file, yielding documents. Supports both XML and GZipped XML files. This is how PubMed documents are stored on the baseline FTP server.
        """
        if str(fname).endswith(".xml"):
            tree = et.parse(fname)
            root = tree.getroot()
        else:
            raise Exception("file type not supported by parser")
        
        for pubmed_article in root.iter("clinical_study"):
            yield parse_ctgov_article(pubmed_article)

    @staticmethod
    def read_folder(folder: Path) -> Iterable[Document]:
        """
        Read a folder of XML files. This method should be used when the CTGOV documents are stored in a folder.
        """
        valid_files = glob.glob(str(folder / "*/*.xml"))
        for file in tqdm(valid_files, desc="folder progress", total=len(valid_files), position=0):
            for article in ClinicalTrialsGovIndexer.read_file(file):
                yield article

    def parse_documents(self, baseline_path: Path) -> Tuple[Iterable[Document], int]:
        total = None
        articles = self.read_folder(baseline_path)
        return articles, total

    def process_document(self, doc: Document) -> Document:
        return doc

    def set_index_fields(self, store_fields: bool = False, optional_fields: List[str] = None):
        self.index.set("id", engine.Field.String, stored=True, docValuesType="sorted")  
        self.index.set("nct_id", engine.Field.String, stored=True, docValuesType="sorted")  
        self.index.set("org_study_id", engine.Field.String, stored=True, docValuesType="sorted")  
        self.index.set("secondary_id", engine.Field.String, stored=True, docValuesType="sorted")  
        
        self.index.set("verification_date", engine.DateTimeField, stored=store_fields)
        self.index.set("study_first_submitted", engine.DateTimeField, stored=store_fields)
        self.index.set("study_first_submitted_qc", engine.DateTimeField, stored=store_fields)
        self.index.set("study_first_posted", engine.DateTimeField, stored=store_fields)
        self.index.set("last_update_submitted", engine.DateTimeField, stored=store_fields)
        self.index.set("last_update_submitted_qc", engine.DateTimeField, stored=store_fields)
        self.index.set("last_update_posted", engine.DateTimeField, stored=store_fields)
        
        self.index.set("brief_title", engine.Field.Text, stored=store_fields)
        self.index.set("official_title", engine.Field.Text, stored=store_fields)
        self.index.set("brief_summary", engine.Field.Text, stored=store_fields)
        self.index.set("source", engine.Field.Text, stored=store_fields)
        self.index.set("detailed_description", engine.Field.Text, stored=store_fields)
        self.index.set("overall_status", engine.Field.Text, stored=store_fields)
        self.index.set("phase", engine.Field.Text, stored=store_fields)
        self.index.set("study_type", engine.Field.Text, stored=store_fields)
        self.index.set("has_expanded_access", engine.Field.Text, stored=store_fields)
        self.index.set("primary_outcome", engine.Field.Text, stored=store_fields)
        self.index.set("condition", engine.Field.Text, stored=store_fields)
        self.index.set("intervention", engine.Field.Text, stored=store_fields)
        self.index.set("criteria", engine.Field.Text, stored=store_fields)
        self.index.set("gender", engine.Field.Text, stored=store_fields)
        self.index.set("healthy_volunteers", engine.Field.Text, stored=store_fields)
        self.index.set("location", engine.Field.Text, stored=store_fields)

        self.index.set("minimum_age", engine.Field, stored=store_fields)
        self.index.set("maximum_age", engine.Field, stored=store_fields)

        self.index.set("sponsors", engine.Field.String, stored=store_fields)
        self.index.set("secondary_outcomes", engine.Field.String, stored=store_fields)
        self.index.set("location_countries", engine.Field.String, stored=store_fields)
        self.index.set("keyword", engine.Field.String, stored=store_fields)
        self.index.set("intervention_browse", engine.Field.String, stored=store_fields)
        self.index.set("condition_browse", engine.Field.String, stored=store_fields)

    def search(self, query: str, n_hits=10) -> List[Document]:
        hits = self.index.search(query, scores=False, mincount=n_hits)
        if n_hits is None:
            n_hits = len(hits)
        for hit in hits[:n_hits]:
            if self.store_fields:
                yield ClinicalTrialsGovArticle.from_dict(hit.dict())

    def search_fmt(self, query: str, n_hits=10, hit_formatter: str = None):
        hits = self.index.search(query, scores=False, mincount=n_hits)
        print(f"hits: {len(hits)}")
        for hit in hits[:n_hits]:
            article = ClinicalTrialsGovArticle.from_dict(hit.dict())
            print(article.id)
        print("====================")
