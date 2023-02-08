"""
Provides all the functionality for indexing and searching documents.
It includes the Document class, which is used to represent documents in pybool_ir.
It also includes generic and off-the-shelf indexing pipelines.
"""

from .index import Indexer

__all__ = ["Indexer"]
