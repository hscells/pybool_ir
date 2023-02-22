from pathlib import Path
from typing import Iterable, List, Union

import ir_datasets
from lupyne import engine

from pybool_ir.index import Indexer
from pybool_ir.index.document import Document


class IRDatasetsIndexer(Indexer):

    def __init__(self, index_path: Union[Path, str], dataset_name: str,
                 store_fields: bool = True, optional_fields: List[str] = None):
        super().__init__(index_path, store_fields, optional_fields)
        self.dataset_name = dataset_name
        self.dataset = ir_datasets.load(dataset_name)

    def process_document(self, doc: Document) -> Document:
        return doc

    def parse_documents(self) -> (Iterable[Document], int):
        def _doc_iter():
            for doc in self.dataset.docs_iter():
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
        self.index.set("contents", engine.Field.Text, stored=store_fields)
        for field in self.dataset.docs_cls()._fields:
            if field not in ["id", "date", "contents"]:
                self.index.set(field, engine.Field.Text, stored=store_fields)

    def bulk_index(self):
        articles, total = self.parse_documents()
        self._bulk_index(articles, total=total)
