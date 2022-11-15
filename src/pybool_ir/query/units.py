from abc import abstractmethod

import lucene
from lupyne import engine
# noinspection PyUnresolvedReferences
from org.apache.lucene import search

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
search.BooleanQuery.setMaxClauseCount(4096)  # There is apparently a cap for efficiency reasons.
analyzer = engine.analyzers.Analyzer.standard()


class UnitAtom(object):
    @property
    @abstractmethod
    def query(self) -> str:
        raise NotImplementedError()

    @property
    def analyzed_query(self) -> str:
        return analyzer.parse(self.query).__str__()

    @property
    @abstractmethod
    def raw_query(self) -> str:
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_str(cls, s: str) -> "UnitAtom":
        raise NotImplementedError()


class QueryAtom(UnitAtom):
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
        return cls([s])

    def __repr__(self):
        return f"Phrase({self.query})"
