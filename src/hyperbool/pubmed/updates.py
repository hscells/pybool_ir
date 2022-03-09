import xml.etree.ElementTree as et
from xml.etree.ElementTree import Element
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class PubmedArticle:
    pmid: str
    # date_completed: date
    # date_revised: date = field(init=False)
    article_date: datetime
    article_title: str
    abstract: str
    publication_type: List[str]
    mesh_heading_list: List[str]
    keyword_list: List[str]


def parse_pubmed_article_node(element: Element) -> PubmedArticle:
    article_date_element = element.find("MedlineCitation/Article/ArticleDate")
    return PubmedArticle(
        pmid=element.find("MedlineCitation/PMID").text,
        article_date=datetime(year=int(article_date_element.find("Year").text),
                              month=int(article_date_element.find("Month").text),
                              day=int(article_date_element.find("Day").text)),
        article_title=element.find("MedlineCitation/Article/ArticleTitle").text,
        abstract=" ".join([el.text for el in element.findall("MedlineCitation/Article/Abstract/AbstractText")]),
        publication_type=[el.text for el in element.findall("MedlineCitation/Article/PublicationTypeList/PublicationType")],
        mesh_heading_list=[el.text for el in element.findall("MedlineCitation/MeshHeadingList/MeshHeading/DescriptorName")],
        keyword_list=[el.text for el in element.findall("MedlineCitation/KeywordList/Keyword")],
    )


def load_updates(fname: str) -> List[PubmedArticle]:
    tree = et.parse(fname)
    root = tree.getroot()
    for pubmed_article in root.iter("PubmedArticle"):
        yield parse_pubmed_article_node(pubmed_article)
