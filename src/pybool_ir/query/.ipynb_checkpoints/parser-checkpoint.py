"""
Base classes for representing queries.
"""

from abc import ABC, abstractmethod

import lucene
from lupyne import engine

from pybool_ir.query.ast import ASTNode

MAX_CLAUSES = 60_000

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query


class QueryParser(ABC):
    """
    Base class for implementing query parsers.
    A query parser should be able to parse a raw query into an AST node, and then format that AST node into a lucene query.
    """

    @classmethod
    def default_field(cls) -> str:
        return "contents"

    @abstractmethod
    def parse_lucene(self, raw_query: str) -> Q:
        """
          Parse a raw query into a lucene query.
          """
        raise NotImplementedError()

    @abstractmethod
    def parse_ast(self, raw_query: str) -> ASTNode:
        """
          Parse a raw query into an AST node.
          """
        raise NotImplementedError()

    @abstractmethod
    def format(self, node: ASTNode) -> str:
        """
          Format an AST node into a raw query.
          """
        raise NotImplementedError()

    def transform(self, node: ASTNode) -> Q:
        """
          Transform an AST node into a lucene query.
          """
        return self.parse_lucene(self.format(node))
