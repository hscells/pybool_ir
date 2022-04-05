import gzip
import os
import xml.etree.ElementTree as et
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List
from xml.etree.ElementTree import Element

import lucene
from dataclasses_json import dataclass_json
from lupyne import engine
from tqdm.auto import tqdm

from hyperbool.pubmed.mesh import analyze_mesh

assert lucene.getVMEnv() or lucene.initVM()


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


def month_str_to_month(month_str: str) -> int:
    # If we have a digit, just return it and assume month.
    if month_str.isdigit():
        return int(month_str)

    # Otherwise, first we need to make the string lower case.
    month_str = month_str.lower()

    # Publication dates without a month are set to January, multiple
    # months (e.g., Oct-Dec) are set to the first month.
    parts = month_str.split("-")
    if len(parts) > 0:
        month_str = parts[0]

    # Journals vary in the way the publication date appears on an issue.
    # Some journals include just the year, whereas others include the year
    # plus month or year plus month plus day. And, some journals use the
    # year and season (e.g., Winter 1997). The publication date in the
    # citation is recorded as it appears in the journal.
    years = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    # Dates with a season are set as:
    # winter = January, spring = April, summer = July and fall = October.
    seasons = {"winter": 1, "spring": 4, "summer": 7, "fall": 10}

    possible_months = {**years, **seasons}
    if month_str in possible_months:
        return possible_months[month_str]

    raise KeyError(f"{month_str} is not a parseable string")


def parse_pubmed_article_node(element: Element) -> PubmedArticle:
    # https://pubmed.ncbi.nlm.nih.gov/help/#dp
    # article_date_element = element.find("PubmedData/History/PubMedPubDate[@PubStatus='pubmed']")
    # datetime(year=int(article_date_element.find("Year").text),
    #          month=int(article_date_element.find("Month").text),
    #          day=int(article_date_element.find("Day").text))
    article_date: datetime

    journal_date_element = element.find("MedlineCitation/Article/Journal/JournalIssue/PubDate")
    if journal_date_element is not None:
        medline_date = journal_date_element.find("MedlineDate")
        if medline_date:
            year, month = medline_date.text.split()
            day = 1
        else:
            year = journal_date_element.find("Year")
            month = journal_date_element.find("Month") or journal_date_element.find("Season")
            day = journal_date_element.find("Day")
        article_date = datetime(year=int(year.text) if year is not None else 1960,
                                month=month_str_to_month(month.text) if month else 1,
                                day=int(day.text) if day is not None else 1)
    abstract_el = element.findall("MedlineCitation/Article/Abstract/AbstractText")
    return PubmedArticle(
        pmid=element.find("MedlineCitation/PMID").text,
        date=article_date,
        # date_revised=field_data.YES if article_revised_element is not None else field_data.NO,
        title="".join(element.find("MedlineCitation/Article/ArticleTitle").itertext()),
        abstract=" ".join(["".join(x.itertext()) for x in abstract_el]) if abstract_el is not None else "",
        publication_type=[el.text for el in element.findall("MedlineCitation/Article/PublicationTypeList/PublicationType")],
        mesh_heading_list=[analyze_mesh(el.text) for el in element.findall("MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName")],
        mesh_major_heading_list=[analyze_mesh(el.text) for el in element.findall("MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName[@MajorTopicYN='Y']")],
        mesh_qualifier_list=[analyze_mesh(el.text) for el in element.findall("MedlineCitation/MeshHeadingList/MeshHeading/QualifierName")],
        keyword_list=[el.text for el in element.findall("MedlineCitation/KeywordList/Keyword")],
    )


def read_file(fname: Path) -> List[PubmedArticle]:
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


def read_folder(folder: Path) -> List[PubmedArticle]:
    valid_files = [f for f in os.listdir(str(folder)) if not f.startswith(".")]
    for file in tqdm(valid_files, desc="folder progress", total=len(valid_files), position=0):
        for article in read_file(folder / file):
            yield article


def set_index_fields(indexer: engine.Indexer):
    indexer.set("pmid", engine.Field.String, stored=True, docValuesType="sorted")
    indexer.set("date", engine.DateTimeField, stored=True)
    indexer.set("title", engine.Field.Text, stored=True)
    indexer.set("abstract", engine.Field.Text, stored=True)
    indexer.set("publication_type", engine.Field.String, stored=True)
    indexer.set("mesh_heading_list", engine.Field.String, stored=True)
    indexer.set("mesh_major_heading_list", engine.Field.String, stored=True)
    indexer.set("mesh_qualifier_list", engine.Field.String, stored=True)
    indexer.set("keyword_list", engine.Field.String, stored=True)


def load_mem_index() -> engine.Indexer:
    indexer = engine.Indexer(directory=None)
    set_index_fields(indexer)
    return indexer


def load_index(path: Path) -> engine.Indexer:
    indexer = engine.Indexer(directory=str(path))
    set_index_fields(indexer)
    return indexer


def add_document(indexer: engine.IndexWriter, doc: PubmedArticle) -> None:
    # if doc.date_revised == field_data.YES:
    #     indexer.delete(f"pmid:{doc.pmid}")
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


def bulk_index(indexer: engine.IndexWriter, docs: List[PubmedArticle]) -> None:
    for i, doc in tqdm(enumerate(docs), desc="indexing progress", position=1, total=None):
        add_document(indexer, doc)
    indexer.commit()


class Index:
    def __init__(self, index_path: Path):
        self.index_path = index_path
        self.index: engine.Indexer

    def bulk_index(self, baseline_path: Path):
        articles = read_folder(baseline_path)
        bulk_index(self.index, articles)

    def search(self, query: str, n_hits=10,
               hit_formatter: str = "{pmid} {title}\n{date}\n{mesh_major_heading_list}\n{mesh_heading_list}\n{mesh_qualifier_list}\n--------------------"):
        hits = self.index.search(query, scores=False,  mincount=n_hits)
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

    def __enter__(self):
        self.index = load_index(self.index_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.index.close()
