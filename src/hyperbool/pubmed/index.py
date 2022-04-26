import calendar
import gzip
import os
import xml.etree.ElementTree as et
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Iterable
from xml.etree.ElementTree import Element

import lucene
from dataclasses_json import dataclass_json
from lupyne import engine
from tqdm.auto import tqdm

assert lucene.getVMEnv() or lucene.initVM()

DEFAULT_YEAR = 1900
DEFAULT_MONTH = 1
DEFAULT_DAY = 1


@dataclass_json
@dataclass
class PubmedArticle:
    pmid: str
    date: datetime
    title: str
    abstract: str
    publication_type: List[str]
    mesh_heading_list: List[str]
    mesh_qualifier_list: List[str]
    mesh_major_heading_list: List[str]
    keyword_list: List[str]


possible_days = {"1st": "1", "2nd": "2", "3rd": "3"}
for _i in range(31):
    str_i = str(_i)
    possible_days[f"{_i}th"] = str_i


def day_str_to_day(day_str: str) -> Tuple[str, bool]:
    if day_str.isdigit():
        return day_str, True
    if day_str in possible_days:
        return possible_days[day_str], True
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


def month_str_to_month(month_str: str, fail_on_nonparseable_str: bool = False) -> int:
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
    day, is_day = day_str_to_day(day)
    if not is_day:  # Hmm, maybe the day isn't a day.
        # Therefore, the day is likely a month!
        day, month = month, day
        day, _ = day_str_to_day(day)

    # Hail Mary at this point, who knows what the day is!
    if not day.isdigit():
        day = str(DEFAULT_DAY)

    # Now we can convert the parts to ints for our proper datetime object.
    year = int(year) if year is not None else DEFAULT_YEAR
    month = month_str_to_month(month) if month is not None else DEFAULT_MONTH  # Special parsing for months.
    day = int(day) if day is not None else DEFAULT_DAY
    return year, month, day


def parse_pubmed_article_node(element: Element) -> PubmedArticle:
    pmid = element.find("MedlineCitation/PMID").text

    # The following is a needlessly complicated, yet accurate implementation
    # of how Pubmed handles the publication date of documents.
    # For more information about the nuances of this technical marvel, see:
    # https://pubmed.ncbi.nlm.nih.gov/help/#dp
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
            month = month_str_to_month(month.text) if month is not None else DEFAULT_MONTH
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
    return PubmedArticle(
        pmid=pmid,
        date=article_date,
        # date_revised=field_data.YES if article_revised_element is not None else field_data.NO,
        title="".join(element.find("MedlineCitation/Article/ArticleTitle").itertext()),
        abstract=" ".join(["".join(x.itertext()) for x in abstract_el])
        if abstract_el is not None
        else "",
        publication_type=[
            el.text
            for el in element.findall(
                "MedlineCitation/Article/PublicationTypeList/PublicationType"
            )
        ],
        mesh_heading_list=[
            # analyze_mesh(el.text)
            el.text
            for el in element.findall(
                "MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName"
            )
        ],
        mesh_major_heading_list=[
            # analyze_mesh(el.text)
            el.text
            for el in element.findall(
                "MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName[@MajorTopicYN='Y']"
            )
        ],
        mesh_qualifier_list=[
            # analyze_mesh(el.text)
            el.text
            for el in element.findall(
                "MedlineCitation/MeshHeadingList/MeshHeading/QualifierName"
            )
        ],
        keyword_list=[
            el.text for el in element.findall("MedlineCitation/KeywordList/Keyword")
        ],
    )


def set_index_fields(indexer: engine.Indexer):
    indexer.set("pmid", engine.Field.String, stored=True, docValuesType="sorted")
    indexer.set("date", engine.DateTimeField, stored=True)
    indexer.set("title", engine.Field.Text, stored=True)
    indexer.set("abstract", engine.Field.Text, stored=True)
    indexer.set("keyword_list", engine.Field.Text, stored=True)
    indexer.set("publication_type", engine.Field.Text, stored=True)
    indexer.set("mesh_heading_list", engine.Field.String, stored=True)
    indexer.set("mesh_qualifier_list", engine.Field.String, stored=True)
    indexer.set("mesh_major_heading_list", engine.Field.String, stored=True)


def load_mem_index() -> engine.Indexer:
    indexer = engine.Indexer(directory=None)
    set_index_fields(indexer)
    return indexer


def load_index(path: Path) -> engine.Indexer:
    indexer = engine.Indexer(directory=str(path))
    set_index_fields(indexer)
    return indexer


def add_document(indexer: engine.IndexWriter, doc: PubmedArticle) -> None:
    if doc.keyword_list is None or not all(doc.keyword_list):
        doc.keyword_list = []
    if doc.mesh_heading_list is None or not all(doc.mesh_heading_list):
        doc.mesh_heading_list = []
    if doc.mesh_major_heading_list is None or not all(doc.mesh_major_heading_list):
        doc.mesh_heading_list = []
    if doc.mesh_qualifier_list is None or not all(doc.mesh_qualifier_list):
        doc.mesh_heading_list = []
    if doc.publication_type is None or not all(doc.publication_type):
        doc.publication_type = []
    indexer.add(doc.to_dict())


def read_file(fname: Path) -> Iterable[PubmedArticle]:
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


def read_folder(folder: Path) -> Iterable[PubmedArticle]:
    valid_files = [f for f in os.listdir(str(folder)) if not f.startswith(".")]
    for file in tqdm(valid_files, desc="folder progress", total=len(valid_files), position=0):
        print(file)
        for article in read_file(folder / file):
            yield article


def read_jsonl(file: Path) -> Iterable[PubmedArticle]:
    with open(file, "r") as f:
        for line in f:
            yield PubmedArticle.from_json(line)


def bulk_index(indexer: engine.IndexWriter, docs: Iterable[PubmedArticle]) -> None:
    for i, doc in tqdm(enumerate(docs), desc="indexing progress", position=1, total=None):
        add_document(indexer, doc)
        if i % 100_000 == 0:
            indexer.commit()
    indexer.commit()


class Index:
    def __init__(self, index_path: Path):
        self.index_path = index_path
        self.index: engine.Indexer

    def bulk_index(self, baseline_path: Path):
        # !! WARNING TO ALL YE WHO VISIT ME HERE !!
        # Unfortunately, despite about three days of work to try
        # making this function concurrent, it seems like there are
        # too many factors that make it almost impossible.
        #   1. async methods cannot deal with the time it takes to open files.
        #   2. pylucne is not thread safe (?); indexing must be synchronous.
        assert isinstance(baseline_path, Path)
        if baseline_path.is_dir():
            articles = read_folder(baseline_path)
        else:
            articles = read_jsonl(baseline_path)
        bulk_index(self.index, articles)

    def search(self, query: str, n_hits=10,
               hit_formatter: str = "{pmid} {title}\n{date}\n{mesh_major_heading_list}\n{mesh_heading_list}\n{mesh_qualifier_list}\n--------------------"):
        hits = self.index.search(query, scores=False, mincount=n_hits)
        print(len(hits))
        for hit in hits[:n_hits]:
            article: PubmedArticle = PubmedArticle.from_dict(hit.dict("mesh_heading_list",
                                                                      "mesh_qualifier_list",
                                                                      "mesh_major_heading_list",
                                                                      "keyword_list",
                                                                      "publication_type"))
            print(hit_formatter.format(pmid=article.pmid,
                                       title=article.title,
                                       date=article.date,
                                       mesh_heading_list=article.mesh_heading_list,
                                       mesh_qualifier_list=article.mesh_qualifier_list,
                                       mesh_major_heading_list=article.mesh_major_heading_list))

    def retrieve(self, query: str):
        return self.index.search(query, scores=False)

    def __enter__(self):
        self.index = load_index(self.index_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.index.close()
