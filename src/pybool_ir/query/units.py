"""
Internal representations for parsing queries. These classes help with the conversion of raw queries into AST nodes and lucene queries.
"""

from abc import abstractmethod

import lucene
from lupyne import engine
# noinspection PyUnresolvedReferences
from org.apache.lucene import search

from pybool_ir.query.parser import MAX_CLAUSES

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
# TODO https://lucene.apache.org/core/10_0_0/MIGRATE.html
#search.BooleanQuery.setMaxClauseCount(MAX_CLAUSES)  # There is apparently a cap for efficiency reasons.
analyzer = engine.analyzers.Analyzer.standard()


class UnitAtom(object):
    """
    A unit is the base class that represents a single query atom. There can be different kinds of atomic queries, such as a date query or a term query.
    As such, this class is the parent class for all these kinds of atomic queries.
    """
    @property
    @abstractmethod
    def query(self) -> str:
        """
        The query that is to be analyzed and then used to create a Lucene query.
        """
        raise NotImplementedError()

    @property
    def analyzed_query(self) -> str:
        """
        The final, analyzed query that can be used to search with a Lucene index.
        """
        # Although possible for query languages to include such characters inside queries,
        # these appear to be special Lucene characters, and so must be replaced prior to analysis.
        query = self.query.replace("[", " ").replace("]", " ").replace("/", " ")
        return analyzer.parse(query).__str__()

    @property
    @abstractmethod
    def raw_query(self) -> str:
        """
        The underlying, printable query. This is often identical to the `query` property, but can be different for some query languages.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_str(cls, s: str) -> "UnitAtom":
        raise NotImplementedError()


class QueryAtom(UnitAtom):
    """
    Helper class for parsing queries that do not need any additional parsing, like term or phrase queries.
    For examples of other kinds of queries and how they might be implemented,
    take a look at the private classes from the `pybool_ir.query.pubmed.parser` module.
    """
    def __init__(self, tokens):
        self._raw_query = tokens[0]
        self._query = tokens[0]  # analyzer.parse(tokens[0]).__str__()
        self.quoted = True if self._query.startswith('"') and self._query.endswith('"') else False
        self.fuzzy = True if "*" in tokens[0] else False

        if self.quoted:
            self._query = self._query[1:-1]

    @property
    def query(self):
        return self._query

    @property
    def raw_query(self):
        return self._raw_query

    @classmethod
    def from_str(cls, s: str) -> "QueryAtom":
        """
        Convenience method for creating a `QueryAtom` from a string.
        """
        return cls([s])

    def __repr__(self):
        return f"Phrase({self.query})"
