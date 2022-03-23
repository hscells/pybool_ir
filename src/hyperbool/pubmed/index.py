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
from tqdm import tqdm

from hyperbool.pubmed import field_data

assert lucene.getVMEnv() or lucene.initVM()


@dataclass_json
@dataclass
class PubmedArticle:
    pmid: str
    # date_completed: date
    date: datetime
    # date_revised: str
    title: str
    abstract: str
    publication_type: List[str]
    mesh_heading_list: List[str]
    keyword_list: List[str]


def parse_pubmed_article_node(element: Element) -> PubmedArticle:
    article_date_element = element.find("PubmedData/History/PubMedPubDate[@PubStatus='pubmed']")
    article_revised_element = element.find("MedlineCitation/DateRevised")
    abstract_el = element.findall("MedlineCitation/Article/Abstract/AbstractText")
    return PubmedArticle(
        pmid=element.find("MedlineCitation/PMID").text,
        date=datetime(year=int(article_date_element.find("Year").text),
                      month=int(article_date_element.find("Month").text),
                      day=int(article_date_element.find("Day").text)),
        # date_revised=field_data.YES if article_revised_element is not None else field_data.NO,
        title="".join(element.find("MedlineCitation/Article/ArticleTitle").itertext()),
        abstract=" ".join(["".join(x.itertext()) for x in abstract_el]) if abstract_el is not None else "",
        publication_type=[el.text for el in element.findall("MedlineCitation/Article/PublicationTypeList/PublicationType")],
        mesh_heading_list=[el.text for el in element.findall("MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName")],
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
    for file in os.listdir(str(folder)):
        if file.startswith("."):
            continue
        for article in read_file(folder / file):
            yield article


def set_index_fields(indexer: engine.Indexer):
    indexer.set("pmid", engine.Field.String, stored=True)
    indexer.set("date", engine.DateTimeField, stored=True)
    # indexer.set("date_revised", engine.Field.String)
    indexer.set("title", engine.Field.Text, stored=True)
    indexer.set("abstract", engine.Field.Text, stored=True)
    indexer.set("publication_type", engine.Field.Text, stored=True)
    indexer.set("mesh_heading_list", engine.Field.Text, stored=True)
    indexer.set("keyword_list", engine.Field.Text, stored=True)


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
    if doc.publication_type is None or not all(doc.publication_type):
        doc.publication_type = []
    indexer.add(doc.to_dict())


def bulk_index(indexer: engine.IndexWriter, docs: List[PubmedArticle]) -> None:
    for i, doc in tqdm(enumerate(docs)):
        add_document(indexer, doc)
    indexer.commit()
# %%
