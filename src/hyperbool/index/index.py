from pathlib import Path
from typing import List, Iterable, Dict, Callable, Any

import lucene
from lupyne import engine
from tqdm.auto import tqdm
from abc import ABC, abstractmethod

from hyperbool.index.document import Document

assert lucene.getVMEnv() or lucene.initVM()


class Indexer(ABC):
    def __init__(self, index_path: Path, store_fields: bool = False, optional_fields: List[str] = None):
        self.index_path = index_path
        self.store_fields = store_fields
        self.optional_fields = optional_fields
        self.index: engine.Indexer

    def retrieve(self, query: str):
        return self.index.search(query, scores=False)

    def add_document(self, doc: Document, optional_fields: Dict[str, Callable[[Document], Any]] = None) -> None:
        if optional_fields is not None:
            for optional_field_name, optional_field_func in optional_fields.items():
                doc.set(optional_field_name, optional_field_func(doc))
        try:
            self.index.add(doc)
        except Exception as e:
            print(doc)
            raise e

    @abstractmethod
    def process_document(self, doc: Document) -> Document:
        """Get a document ready for indexing."""

    @abstractmethod
    def parse_documents(self, fname: Path) -> (Iterable[Document], int):
        """Return an iterable of documents from a path."""

    @abstractmethod
    def set_index_fields(self, store_fields: bool = False, optional_fields: List[str] = None):
        """Set fields of the index."""

    def bulk_index(self, fname: Path, optional_fields: Dict[str, Callable[[Document], Any]] = None):
        assert isinstance(fname, Path)
        articles, total = self.parse_documents(fname)
        self._bulk_index(articles, total=total, optional_fields=optional_fields)

    def _bulk_index(self, docs: Iterable[Document], total=None, optional_fields: Dict[str, Callable[[Document], Any]] = None) -> None:
        for i, doc in tqdm(enumerate(docs), desc="indexing progress", position=1, total=total):
            self.add_document(self.process_document(doc), optional_fields)
            if i % 100_000 == 0:
                self.index.commit()
        self.index.commit()

    def _set_index_fields(self, optional_fields: List[str]):
        if optional_fields is not None:
            for optional_field_name in optional_fields:
                self.index.set(optional_field_name, engine.Field.Text, stored=self.store_fields)

    def __enter__(self):
        self.index = engine.Indexer(directory=str(self.index_path))
        self.set_index_fields(store_fields=self.store_fields, optional_fields=self.optional_fields)
        self._set_index_fields(self.optional_fields)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.index.close()
