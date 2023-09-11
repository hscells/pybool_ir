"""
Generic indexers and searchers for JSONL and JSONLD files.
"""

import json
from pathlib import Path
from typing import List, Iterable, Union

from pybool_ir.index.document import Document
from pybool_ir.index.index import Indexer, SearcherMixin

import lucene
from lupyne import engine

from pybool_ir.query.generic.parser import DEFAULT_FIELD
from pybool_ir.util import StopFilter, TypeAsPayloadTokenFilter

# noinspection PyUnresolvedReferences
from org.apache.lucene.analysis.en import PorterStemFilter

assert lucene.getVMEnv() or lucene.initVM()


class JsonlIndexer(Indexer):
    """
    Generic indexer for JSONL files. The JSONL file should contain one JSON object per line.

    Each document must have an `id` and `date` field.
    """

    def __init__(self, index_path: Union[Path, str],
                 store_fields: bool = True, store_termvectors: bool = False, optional_fields: List[str] = None):
        super().__init__(index_path, store_fields, store_termvectors, optional_fields)
        # Do some more general purpose analysis.
        self._analyzer = engine.Analyzer.standard(StopFilter, PorterStemFilter, TypeAsPayloadTokenFilter)

    def process_document(self, doc: Document) -> Document:
        assert (doc.has_key("id") and doc.has_key("date"))
        return doc

    def parse_documents(self, fname: Path) -> (Iterable[Document], int):
        with open(fname, "r") as f:
            total = sum(1 for _ in f)

        def read_jsonl() -> Iterable[Document]:
            with open(fname, "r") as f:
                for line in f:
                    yield Document.from_json(line)

        return read_jsonl(), total

    def set_index_fields(self, store_fields: bool = False):
        self.index.set("id", engine.Field.String, stored=True, docValuesType="sorted")
        self.index.set("date", engine.DateTimeField, stored=store_fields)


class JsonldIndexer(JsonlIndexer):
    """
    Generic indexer for JSONLD files. The JSONLD file should contain one JSON object per line.
    This indexer assumes that the first line of the file is the document ID, and the second line is the document datasets.
    This class can be used to index datasets in the same way ElasticSearch does.

    Each document must have an `id` and `date` field.
    """

    def parse_documents(self, fname: Path) -> (Iterable[Document], int):
        with open(fname, "r") as f:
            total = sum(1 for _ in f)

        def read_jsonld() -> Iterable[Document]:
            with open(fname, "r") as f:
                for i, line in enumerate(f):
                    if i % 2 == 0:
                        doc_id = json.loads(line)
                    else:
                        data = json.loads(line)
                        data["id"] = doc_id
                        yield Document.from_dict(data)

        return read_jsonld(), total


class GenericSearcher(Indexer, SearcherMixin):
    """
    Generic searcher for any kind of index.
    """

    def search(self, query: str, n_hits=10) -> List[Document]:
        hits = self.index.search(query, scores=False, mincount=n_hits)
        if n_hits is None:
            n_hits = len(hits)
        for hit in hits[:n_hits]:
            if self.store_fields:
                article: Document = Document.from_dict(hit.dict())  # TODO: automatically calculate field list.
                yield article
            else:
                yield Document.from_dict(hit.dict())

    def process_document(self, doc: Document) -> Document:
        pass

    def parse_documents(self, fname: Path) -> (Iterable[Document], int):
        pass

    def set_index_fields(self, store_fields: bool = False):
        pass

    def search_fmt(self, query: str, n_hits=10, hit_formatter: str = None):
        if hit_formatter is None and self.store_fields:
            hit_formatter = "{id} {date} " + "{" + DEFAULT_FIELD + "}\n"
        elif hit_formatter is None:
            hit_formatter = "{id} {date}\n"
        hits = self.index.search(query, scores=False, mincount=n_hits)
        print(f"hits: {len(hits)}")
        for hit in hits[:n_hits]:
            print("--------------------")
            if self.store_fields:
                doc = hit.dict()
                del doc["__id__"]
                del doc["__score__"]
                for k, v in doc.items():
                    if isinstance(v, str):
                        if len(v) > 32:
                            v = f"{v[:32]}..."
                    print(f"{k}: {v}")
            else:
                article = Document.from_dict(hit.dict())
                print(hit_formatter.format(id=article.id))
        print("====================")
