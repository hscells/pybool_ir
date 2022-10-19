from pathlib import Path
from typing import List, Iterable

from hyperbool.index.document import Document
from hyperbool.index.index import Indexer

import lucene
from lupyne import engine
from tqdm.auto import tqdm

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
