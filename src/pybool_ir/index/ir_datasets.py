from pathlib import Path
from typing import Iterable, List, Union

import ir_datasets
from ir_datasets.formats import GenericDoc
from lupyne import engine

from pybool_ir.index import Indexer
from pybool_ir.index.document import Document
from pybool_ir.util import TypeAsPayloadTokenFilter, StopFilter
# noinspection PyUnresolvedReferences
from org.apache.lucene.analysis.en import PorterStemFilter


class IRDatasetsIndexer(Indexer):

    def __init__(self, index_path: Union[Path, str], dataset_name: str,
                 store_fields: bool = True, store_termvectors: bool = True, optional_fields: List[str] = None):
        super().__init__(index_path, store_fields=store_fields, store_termvectors=store_termvectors, optional_fields=optional_fields)
        self.dataset_name = dataset_name
        self.dataset = ir_datasets.load(dataset_name)

        # Do some more general purpose analysis.
        self._analyzer = engine.Analyzer.standard(StopFilter, PorterStemFilter, TypeAsPayloadTokenFilter)

    def process_document(self, doc: Document) -> Document:
        return doc

    # noinspection PyMethodOverriding
    def parse_documents(self) -> (Iterable[Document], int):
        def _doc_iter():
            for doc in self.dataset.docs_iter():
                if isinstance(doc, GenericDoc):
                    d = {
                        "id": doc.doc_id,
                        "date": 0,
                        "contents": doc.text,
                    }
                else:
                    d = doc._asdict()
                    if "id" not in d:
                        d["id"] = doc[0]
                    if "date" not in d:
                        d["date"] = 0
                    d["contents"] = [v for v in d.values() if isinstance(v, str)]
                yield Document.from_dict(d)

        return _doc_iter(), self.dataset.docs_count()

    def set_index_fields(self, store_fields: bool = False):
        self.index.set("id", engine.Field.String, stored=True, docValuesType="binary")
        self.index.set("date", engine.DateTimeField, stored=store_fields)
        self.index.set("contents", engine.Field.Text, stored=store_fields, storeTermVectors=self.store_termvectors)
        for field in self.dataset.docs_cls()._fields:
            if field not in ["id", "date", "contents"]:
                self.index.set(field, engine.Field.Text, stored=store_fields, storeTermVectors=self.store_termvectors)

    # noinspection PyMethodOverriding
    def bulk_index(self):
        articles, total = self.parse_documents()
        self._bulk_index(articles, total=total)
