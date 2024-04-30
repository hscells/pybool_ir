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


class PubmedArticle(Document):
    """
    This is a special override of the Document class for PubMed articles. The constructor takes in all the fields that are required for PubMed articles.
    """

    def __init__(self, id: str, date: datetime, title: str, abstract: str,
                 publication_type: List[str], mesh_heading_list: List[str],
                 mesh_qualifier_list: List[str], mesh_major_heading_list: List[str],
                 supplementary_concept_list: List[str], keyword_list: List[str], **optional_fields):
        super().__init__(**{**{
            "id": id,
            "date": date,
            "title": title,
            "abstract": abstract,
            "publication_type": publication_type,
            "mesh_heading_list": mesh_heading_list,
            "mesh_qualifier_list": mesh_qualifier_list,
            "mesh_major_heading_list": mesh_major_heading_list,
            "supplementary_concept_list": supplementary_concept_list,
            "keyword_list": keyword_list
        }, **optional_fields})

    @staticmethod
    def from_hit(hit: Hit):
        """
        Create a PubmedArticle from a lucene Hit. This method also removes the `__id__` and `__score__` fields from the hit.
        A document prior to indexing should be equivalent to a document retrieved from a hit using this method.
        """
        d = hit.dict("mesh_heading_list",
                     "mesh_qualifier_list",
                     "mesh_major_heading_list",
                     "keyword_list",
                     "publication_type",
                     "supplementary_concept_list")
        del d["__id__"]
        del d["__score__"]
        return PubmedArticle.from_dict(d)


_possible_days = {"1st": "1", "2nd": "2", "3rd": "3"}
for _i in range(31):
    str_i = str(_i)
    _possible_days[f"{_i}th"] = str_i


def _day_str_to_day(day_str: str) -> Tuple[str, bool]:
    if day_str.isdigit():
        return day_str, True
    if day_str in _possible_days:
        return _possible_days[day_str], True
    return day_str, False


# Journals vary in the way the publication date appears on an issue.
# Some journals include just the year, whereas others include the year
# plus month or year plus month plus day. And, some journals use the
# year and season (e.g., Winter 1997). The publication date in the
# citation is recorded as it appears in the journal.
_months = {
    # Short English months.
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
    "set": 9,
    # Full English months.
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    # Full German month names.
    "januar": 1,
    "februar": 2,
    "marz": 3,
    "märz": 3,
    "mai": 5,
    "juni": 6,
    "juli": 7,
    "oktober": 10,
    "dezember": 12,
    # Unknown language month names.
    "desember": 12,  # Indonesian?
}

# Dates with a season are set as:
# winter = January, spring = April, summer = July and fall = October.
_seasons = {"winter": 1,
            "spring": 4,
            "summer": 7,
            "fall": 10,
            "autumn": 10,
            "aut": 10,
            "[season]": 1}

# There is no "official" documentation for how these dates are interpreted.
# So let's just use the same dates as the seasons as a pretty good guess.
_quarters = {
    "1st quarter": 1,
    "2nd quarter": 4,
    "3rd quarter": 7,
    "4th quarter": 10,
    "first quarter": 1,
    "second quarter": 4,
    "third quarter": 7,
    "fourth quarter": 10,
}

_possible_months = {**_months, **_seasons, **_quarters}


def _month_str_to_month(month_str: str, fail_on_nonparseable_str: bool = False) -> int:
    # Strange months that fail to parse
    # metaboliche -> ?
    # easter -> ?
    # trimest -> ?
    # qctober -> misspelling of October.
    # oktober -> German for October.
    # suppl -> ?
    # abr -> ?
    # aig -> ?
    # ago -> ?
    # aut -> Autumn (?)
    # dic -> ?
    # mai -> German for May (?)
    # mac -> ?
    # noc -> ?
    # oc -> ?
    # t -> ?
    # c -> ?

    # If we have a digit, just return it and assume month.
    if month_str.isdigit():
        return int(month_str)

    # Otherwise, first we need to make the string lower case.
    month_str = month_str.lower()

    # Publication dates without a month are set to January, multiple
    # months (e.g., Oct-Dec) are set to the first month.
    if "-" in month_str:
        parts = month_str.split("-")
        month_str = parts[0]
    elif "/" in month_str:
        parts = month_str.split("/")
        month_str = parts[0]

    if month_str.isdigit():
        return int(month_str)

    if month_str in _possible_months:
        return _possible_months[month_str]

    for k, v in _possible_months.items():
        if k in month_str:
            return int(v)

    # We may wish to gracefully fail, but if not,
    # just assume the month string is garbage and
    # by default return January as the month.
    err = f"{month_str} is not a parseable string"
    if fail_on_nonparseable_str:
        raise KeyError(err)
    else:
        print(err)
        return 1


def parse_medline_date(date_str: str) -> Tuple[int, int, int]:
    """
    Parse a date string from a Medline record. The returned value is a tuple of (year, month, day).

    The following is a needlessly complicated, yet accurate implementation
    of how Pubmed handles the publication date of documents.
    For more information about the nuances of this technical marvel, see:
    https://pubmed.ncbi.nlm.nih.gov/help/#dp
    """
    day = str(DEFAULT_DAY)
    month = str(DEFAULT_MONTH)
    year = str(DEFAULT_YEAR)
    date_str = date_str.lower(). \
        replace(".", ""). \
        replace(",", ""). \
        replace(" to ", "-"). \
        replace(" & ", "-")
    dur_parts = date_str.split("-")
    # Now that we have the LHS of the duration,
    # let's parse it like normal.
    if len(dur_parts) > 1:
        date_str = dur_parts[0]

    # We now have to split the string and run some
    # tests to determine what the parts of the date are.
    date_parts = date_str.split()

    if len(date_parts) == 1:  # Likely we are looking at a year.
        if date_parts[0].isdigit():  # Almost certainly a year.
            year = date_parts[0]
        else:  # If not, it almost certainly is a month.
            month = date_parts[0]

    elif len(date_parts) == 2:  # Almost certainly a year and a month.
        year, month = date_parts
    elif len(date_parts) >= 3:  # Almost certainly a year-month-day combination.
        year, month, day = date_parts[:3]
        # Oops, some dates look like `2021 4th Quarter`,
        # so we need to do a switch-a-roo.
        if day.lower() == "quarter":
            month = f"{month} {day}"
            day = "1"
    else:
        raise Exception(f"{date_str} is unparseable\nguru meditation: {date_parts}")

    # For the case that the date is something like `Fall 2021`
    # instead of the more typical `2021 Fall`:
    if not year.isdigit():
        year, month = month, year

    # Let's find out if the day could be a day.
    day, is_day = _day_str_to_day(day)
    if not is_day:  # Hmm, maybe the day isn't a day.
        # Therefore, the day is likely a month!
        day, month = month, day
        day, _ = _day_str_to_day(day)

    # Hail Mary at this point, who knows what the day is!
    if not day.isdigit():
        day = str(DEFAULT_DAY)

    # Now we can convert the parts to ints for our proper datetime object.
    year = int(year) if year is not None else DEFAULT_YEAR
    month = _month_str_to_month(month) if month is not None else DEFAULT_MONTH  # Special parsing for months.
    day = int(day) if day is not None else DEFAULT_DAY
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


class PubmedIndexer(Indexer, SearcherMixin):
    """
    Off-the-shelf indexer for Pubmed XML files.

    >>> from pybool_ir.index.pubmed import PubmedIndexer
    >>>
    >>> with PubmedIndexer("path/to/index", store_fields=True) as idx:
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
