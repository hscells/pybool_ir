"""
Base classes for indexing and searching documents.
"""

from pathlib import Path
from typing import List, Iterable, Dict, Callable, Any, Union

import lucene
from lupyne import engine
from tqdm.auto import tqdm
from abc import ABC, abstractmethod
# noinspection PyUnresolvedReferences
from org.apache.lucene import analysis, index
# noinspection PyUnresolvedReferences
from org.apache.lucene.search.similarities import BM25Similarity

from pybool_ir.index.document import Document

assert lucene.getVMEnv() or lucene.initVM()


class Indexer(ABC):
    """
    Base class that provides the basic functionality for indexing and searching documents.
    By default, this class provides no ability to search documents without directly using the lucene API.


    """

    def __init__(self, index_path: Union[Path, str], store_fields: bool = True,
                 store_termvectors: bool = False, optional_fields: List[str] = None):
        if not isinstance(index_path, Path):
            index_path = Path(index_path)
        assert isinstance(index_path, Path)

        self.index_path = index_path
        self.store_fields = store_fields
        self.store_termvectors = store_termvectors
        self.optional_fields = optional_fields

        self._analyzer = analysis.standard.StandardAnalyzer()
        # See: https://lucene.apache.org/core/9_1_0/core/org/apache/lucene/search/similarities/package-summary.html
        self.similarity = BM25Similarity()

        # The underlying lucene index.
        self.index: engine.Indexer

    def retrieve(self, query: str):
        return self.index.search(query, scores=False)

    def add_document(self, doc: Document, optional_fields: Dict[str, Callable[[Document], Any]] = None) -> None:
        """
        Add a single document to the index. This method is called by bulk_index.

        `optional_fields` is a dictionary of field names to functions that take a document and return a value for that field.
        This is useful for adding fields that are not part of the document, but are derived from the document, calculated at index time.
        """
        if optional_fields is not None:
            for optional_field_name, optional_field_func in optional_fields.items():
                doc.set(optional_field_name, optional_field_func(doc))
        try:
            self.index.add(doc)
        except Exception as e:
            print("something was wrong with this document:")
            print(doc)
            raise e

    @abstractmethod
    def process_document(self, doc: Document) -> Document:
        """Get a document ready for indexing."""

    @abstractmethod
    def parse_documents(self, fname: Path) -> (Iterable[Document], int):
        """
        Return an iterable of documents from a path.
        Depending on different ways documents can be stored, indexers might have multiple ways to store files.
        This method chooses the best way to parse a file given the filename.
        """

    @abstractmethod
    def set_index_fields(self, store_fields: bool = False):
        """Set fields of the index. Off-the-shelf implementations of indexing particular collections require specific fields in lucene to be set."""

    def bulk_index(self, fname: Union[Path, str], optional_fields: Dict[str, Callable[[Document], Any]] = None):
        """
        Index a collection of documents from a file or directory.
        """
        if not isinstance(fname, Path):
            fname = Path(fname)
        assert isinstance(fname, Path)
        articles, total = self.parse_documents(fname)
        self._bulk_index(articles, total=total, optional_fields=optional_fields)

    def _bulk_index(self, docs: Iterable[Document], total=None, optional_fields: Dict[str, Callable[[Document], Any]] = None) -> None:
        """
        This is the internal method that actually indexes documents. It will commit the index every 100,000 documents for efficiency.
        """
        for i, doc in tqdm(enumerate(docs), desc="indexing progress", position=1, total=total):
            self.add_document(self.process_document(doc), optional_fields)
            if i % 100_000 == 0:
                self.index.commit()
        self.index.commit()

    def _set_index_fields(self):
        """
        This method sets any optional fields that are specified in the constructor.
        Fields that start with "#" are stored as strings, while other fields are stored as text.
        The difference is that text fields are tokenized, while string fields are not.
        String fields can be used to store arrays of strings, while text fields cannot.
        """
        if self.optional_fields is not None:
            for optional_field_name in self.optional_fields:
                if optional_field_name.startswith("#"):
                    self.index.set(optional_field_name[1:], engine.Field.String, stored=self.store_fields, storeTermVectors=self.store_termvectors)
                else:
                    self.index.set(optional_field_name, engine.Field.Text, stored=self.store_fields, storeTermVectors=self.store_termvectors)

    def set_similarity(self, sim_cls):
        self.similarity = sim_cls
        self.index.setSimilarity(self.similarity)

    def __enter__(self):
        self.index = engine.Indexer(directory=str(self.index_path), nrt=True, analyzer=self._analyzer)
        self.index.setSimilarity(self.similarity)
        self._set_index_fields()
        self.set_index_fields(store_fields=self.store_fields)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.index.close()


class SearcherMixin(ABC):
    """
    Include this mixin to add search functionality to an Indexer.
    """

    @abstractmethod
    def search(self, query: str, n_hits=10) -> List[Document]:
        """
        Given a query, return the top n_hits documents. When n_hits is None, return all documents that match the query.
        """
        pass

    @abstractmethod
    def search_fmt(self, query: str, n_hits=10, hit_formatter: str = None) -> None:
        """
        Perform a search and print the results.
        """
        pass
