from abc import ABC, abstractmethod

import lucene
from lupyne import engine

from pybool_ir.query import ASTNode

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query


class QueryParser(ABC):

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
