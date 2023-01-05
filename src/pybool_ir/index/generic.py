import json
from pathlib import Path
from typing import List, Iterable

from pybool_ir.index.document import Document
from pybool_ir.index.index import Indexer

import lucene
from lupyne import engine

assert lucene.getVMEnv() or lucene.initVM()


class JsonlIndexer(Indexer):
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
