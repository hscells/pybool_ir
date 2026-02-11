"""
Off-the-shelf indexer for PubMed articles.
"""

import calendar
import gzip
import os
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
                    source: str,
                    brief_summary: str,
                    overall_status: str,
                    study_type: str,
                    has_expanded_access: str,
                    condition: str,
                    criteria: str,
                    gender: str,
                    minimum_age: str,
                    maximum_age: str,
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
                    condition_browse: List[str],
        super().__init__(**{**{
            "id": nct_id,
            "org_study_id": org_study_id,
            "secondary_id": secondary_id,
            "nct_id": nct_id,
            "brief_title": brief_title,
            "source": source,
            "brief_summary": brief_summary,
            "overall_status": overall_status,
            "study_type": study_type,
            "has_expanded_access": has_expanded_access,
            "condition": condition,
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
                     "source",
                     "brief_summary",
                     "overall_status",
                     "study_type",
                     "has_expanded_access",
                     "condition",
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
                     "condition_browse")
        del d["__id__"]
        del d["__score__"]
        return ClinicalTrialsGovArticle.from_dict(d)

def parse_ctgov_date(date_str: str) -> Tuple[int, int, int]:
    """
    Parse a date string from a CTGOV record. The returned value is a tuple of (year, month, day).
    """
    day = str(DEFAULT_DAY)
    month = str(DEFAULT_MONTH)
    year = str(DEFAULT_YEAR)
    date_str = date_str.lower(). \
        replace(".", ""). \
        replace(",", ""). \
        replace(" to ", "-"). \
        replace(" & ", "-")

    return year, month, day


def parse_pubmed_article_node(element: Element) -> PubmedArticle:
    """
    Parse a PubmedArticle node from a Pubmed XML element.
    """
    pmid = element.find("MedlineCitation/PMID").text
    article_date: datetime
    journal_date_element = element.find(
        "MedlineCitation/Article/Journal/JournalIssue/PubDate"
    )
    medline_date: Element
    if journal_date_element is not None:
        medline_date = journal_date_element.find("MedlineDate")
        if medline_date is not None:
            year, month, day = parse_medline_date(medline_date.text)
        else:
            year = journal_date_element.find("Year")
            month = journal_date_element.find("Month") or journal_date_element.find("Season")
            day = journal_date_element.find("Day")

            year = int(year.text) if year is not None else DEFAULT_YEAR
            month = _month_str_to_month(month.text) if month is not None else DEFAULT_MONTH
            day = int(day.text) if day is not None else DEFAULT_DAY

        # Okay, *finally* we have integer representations.
        # First, the month could be less than 1. (!?)
        if month < 1:
            month = DEFAULT_MONTH

        # If the "month" is >12, likely the day and month need switching.
        if month > 12:
            month, day = day, month

        # If for some reason, the day exceeds the number of days
        # in a specific month, then just reset the day to the first.
        ndays = calendar.mdays[month] + (month == calendar.February and calendar.isleap(year))
        if day > ndays:
            day = DEFAULT_DAY

        if year < 1700:
            year = DEFAULT_YEAR

        article_date = datetime(year=year, month=month, day=day)
    else:
        raise Exception("no journal date element found")
    abstract_el = element.findall("MedlineCitation/Article/Abstract/AbstractText")
    chemical_list_el = element.findall("MedlineCitation/ChemicalList/Chemical/NameOfSubstance")
    suppl_mesh_list_el = element.findall("MedlineCitation/SupplMeshList/SupplMeshName")
    return PubmedArticle(
        id=pmid,
        date=article_date,
        # date_revised=field_data.YES if article_revised_element is not None else field_data.NO,
        title="".join(element.find("MedlineCitation/Article/ArticleTitle").itertext()),
        abstract=" ".join(["".join(x.itertext()) for x in abstract_el]) if abstract_el is not None else "",
        publication_type=[
            el.text
            for el in element.findall(
                "MedlineCitation/Article/PublicationTypeList/PublicationType"
            )
        ],
        mesh_heading_list=[
            el.text
            for el in element.findall(
                "MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName"
            )
        ],
        mesh_major_heading_list=[
            el.text
            for el in element.findall(
                "MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName[@MajorTopicYN='Y']"
            )
        ],
        mesh_qualifier_list=[
            el.text
            for el in element.findall(
                "MedlineCitation/MeshHeadingList/MeshHeading/QualifierName"
            )
        ],
        supplementary_concept_list=[
                                       el.text
                                       for el in chemical_list_el
                                       if chemical_list_el is not None
                                   ] + [
                                       el.text
                                       for el in suppl_mesh_list_el
                                       if suppl_mesh_list_el is not None
                                   ],
        keyword_list=[
            el.text for el in element.findall("MedlineCitation/KeywordList/Keyword")
        ],
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
        if str(fname).endswith(".gz"):
            with gzip.open(fname, "rb") as f:
                root = et.fromstring(f.read())
        elif str(fname).endswith(".xml"):
            tree = et.parse(fname)
            root = tree.getroot()
        else:
            raise Exception("file type not supported by parser")
        for pubmed_article in root.iter("PubmedArticle"):
            yield parse_pubmed_article_node(pubmed_article)

    @staticmethod
    def read_folder(folder: Path) -> Iterable[Document]:
        """
        Read a folder of XML files. This method should be used when the PubMed documents are stored in a folder.
        """
        valid_files = [f for f in os.listdir(str(folder)) if not f.startswith(".")]
        for file in tqdm(valid_files, desc="folder progress", total=len(valid_files), position=0):
            print(file)
            for article in PubmedIndexer.read_file(folder / file):
                yield article

    @staticmethod
    def read_jsonl(file: Path) -> Iterable[Document]:
        """
        Read a JSONL file. This method should be used when the PubMed documents are stored in a JSONL file.
        The pybool_ir command line tool can be used to convert PubMed XML files to JSONL files.
        Conversion of the files makes indexing considerably faster since the XML files do not need to be parsed.
        """
        with open(file, "r") as f:
            for line in f:
                yield PubmedArticle.from_json(line)

    def parse_documents(self, baseline_path: Path) -> (Iterable[Document], int):
        total = None
        if baseline_path.is_dir():
            articles = self.read_folder(baseline_path)
        else:
            with open(baseline_path) as f:
                total = sum(1 for _ in f)
            articles = PubmedIndexer.read_jsonl(baseline_path)
        return articles, total

    def process_document(self, doc: Document) -> Document:
        if doc.abstract is None:
            doc.abstract = ""

        if doc.title is None:
            doc.title = ""

        # Filter nulls.
        doc.keyword_list = list(filter(None, doc.keyword_list))
        doc.mesh_heading_list = list(filter(None, doc.mesh_heading_list))
        doc.mesh_major_heading_list = list(filter(None, doc.mesh_major_heading_list))
        doc.mesh_qualifier_list = list(filter(None, doc.mesh_qualifier_list))
        doc.supplementary_concept_list = list(filter(None, doc.supplementary_concept_list))
        doc.publication_type = list(filter(None, doc.publication_type))

        # Ensure there are lists and not nulls.
        if doc.keyword_list is None or not all(doc.keyword_list):
            doc.keyword_list = []
        if doc.mesh_heading_list is None or not all(doc.mesh_heading_list):
            doc.mesh_heading_list = []
        if doc.mesh_major_heading_list is None or not all(doc.mesh_major_heading_list):
            doc.mesh_major_heading_list = []
        if doc.mesh_qualifier_list is None or not all(doc.mesh_qualifier_list):
            doc.mesh_qualifier_list = []
        if doc.supplementary_concept_list is None or not all(doc.supplementary_concept_list):
            doc.supplementary_concept_list = []
        if doc.publication_type is None or not all(doc.publication_type):
            doc.publication_type = []

        return doc

    def set_index_fields(self, store_fields: bool = False, optional_fields: List[str] = None):
        self.index.set("id", engine.Field.String, stored=True, docValuesType="sorted")  # PMID
        self.index.set("date", engine.DateTimeField, stored=store_fields)  # Date that the PMID was actually published.
        self.index.set("title", engine.Field.Text, stored=store_fields)
        self.index.set("abstract", engine.Field.Text, stored=store_fields)
        self.index.set("keyword_list", engine.Field.Text, stored=store_fields)
        self.index.set("publication_type", engine.Field.String, stored=store_fields)
        self.index.set("mesh_heading_list", engine.Field.String, stored=store_fields)
        self.index.set("mesh_qualifier_list", engine.Field.String, stored=store_fields)
        self.index.set("mesh_major_heading_list", engine.Field.String, stored=store_fields)
        self.index.set("supplementary_concept_list", engine.Field.String, stored=store_fields)

    def search(self, query: str, n_hits=10) -> List[Document]:
        hits = self.index.search(query, scores=False, mincount=n_hits)
        if n_hits is None:
            n_hits = len(hits)
        for hit in hits[:n_hits]:
            if self.store_fields:
                article: PubmedArticle = PubmedArticle.from_dict(hit.dict("mesh_heading_list",
                                                                          "mesh_qualifier_list",
                                                                          "mesh_major_heading_list",
                                                                          "supplementary_concept_list",
                                                                          "keyword_list",
                                                                          "publication_type"))
                yield article
            else:
                yield PubmedArticle.from_dict(hit.dict())

    def search_fmt(self, query: str, n_hits=10, hit_formatter: str = None):
        if hit_formatter is None and self.store_fields:
            hit_formatter = "--------------------\nPMID * {id} * https://pubmed.ncbi.nlm.nih.gov/{id}\nTITL * {title}\npublished: {date}\nMAJR * {mesh_major_heading_list}\nMESH * {mesh_heading_list}\nQUAL * {mesh_qualifier_list}\nSUPP * {supplementary_concept_list}\nKWRD * {keyword_list}\nPUBT * {publication_type}\n"
        elif hit_formatter is None:
            hit_formatter = "{id} * https://pubmed.ncbi.nlm.nih.gov/{id}"
        hits = self.index.search(query, scores=False, mincount=n_hits)
        print(f"hits: {len(hits)}")
        for hit in hits[:n_hits]:
            if self.store_fields:
                article: PubmedArticle = PubmedArticle.from_dict(hit.dict("mesh_heading_list",
                                                                          "mesh_qualifier_list",
                                                                          "mesh_major_heading_list",
                                                                          "supplementary_concept_list",
                                                                          "keyword_list",
                                                                          "publication_type"))
                print(hit_formatter.format(id=article.id,
                                           title=article.title,
                                           date=article.date,
                                           mesh_heading_list=article.mesh_heading_list,
                                           mesh_qualifier_list=article.mesh_qualifier_list,
                                           supplementary_concept_list=article.supplementary_concept_list,
                                           keyword_list=article.keyword_list,
                                           publication_type=article.publication_type,
                                           mesh_major_heading_list=article.mesh_major_heading_list))
            else:
                article = PubmedArticle.from_dict(hit.dict())
                print(hit_formatter.format(id=article.id))
        print("====================")
