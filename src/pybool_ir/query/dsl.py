from abc import ABC, abstractmethod
from typing import Union, List

import lucene
from lupyne import engine
# noinspection PyUnresolvedReferences
from org.apache.lucene import search

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query

default_field = "contents"
op_and = "and"
op_or = "or"
op_not = "not"


class QueryObject(ABC):
    @abstractmethod
    def eval(self):
        raise NotImplementedError()

    def __and__(self, other):
        if not isinstance(other, QueryObject):
            other = auto(other)
        if isinstance(self, Op) and self._op == op_and:
            self.children.append(other)
            return self
        return AND(self, other)

    def __rand__(self, other):
        return self.__and__(other)

    def __or__(self, other):
        if isinstance(self, Op) and self._op == op_or:
            self.children.append(other)
            return self
        return OR(self, other)

    def __ror__(self, other):
        return self.__or__(other)

    def replace(self, other):
        assert isinstance(other, QueryObject), "Can only replace with another QueryObject."
        self.__dict__.update(other.__dict__)
        return self

    @abstractmethod
    def accept(self, visitor):
        raise NotImplementedError()


class AtomicQueryObject(QueryObject, ABC):
    def __init__(self, query: str = None, field=default_field):
        self.query = query
        self.field = field

    def __getitem__(self, term: Union[str, slice], field: str = default_field):
        if isinstance(term, slice):
            field = term.stop
            term = term.start
        return Term(term, field)

    def __call__(self, *args):
        return self.__getitem__(*args)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.query}:{self.field})"


class Term(AtomicQueryObject):

    def __init__(self, term: str = None, field=default_field):
        assert term is None or " " not in term, "Term cannot contain spaces."
        super().__init__(term, field)

    def eval(self):
        return Q.term(self.field, self.query)

    def accept(self, visitor):
        visitor.visit(self)


class Phrase(AtomicQueryObject):

    def __init__(self, phrase: str, field=default_field):
        super().__init__(query=phrase, field=field)

    def eval(self):
        return Q.phrase(self.field, self.query)

    def accept(self, visitor):
        visitor.visit(self)


def auto(query: str, field: str = default_field):
    if " " in query:
        return Phrase(query, field)
    return Term(query, field)


class Op(QueryObject):
    def __init__(self, op: str, *args: Union[str, QueryObject]):
        self._op = op.lower()
        self.children = []
        for arg in args:
            if isinstance(arg, str):
                arg = auto(arg)
            self.children.append(arg)

    def __repr__(self):
        return f"{self._op}({', '.join([str(c) for c in self.children])})"

    def eval(self):
        if self._op == "and":
            return Q.all(*[c.eval() for c in self.children])
        elif self._op == "or":
            return Q.any(*[c.eval() for c in self.children])
        elif self._op == "not":
            builder = search.BooleanQuery.Builder()
            builder.add(self.children[0], search.BooleanClause.Occur.SHOULD)
            builder.add(self.children[1], search.BooleanClause.Occur.MUST_NOT)
            return builder.build()
        raise NotImplementedError()

    def accept(self, visitor):
        visitor.visit(self)
        for child in self.children:
            child.accept(visitor)


class QueryVisitor(ABC):
    def __init__(self, query: QueryObject):
        query.accept(self)

    @abstractmethod
    def visit(self, query: QueryObject):
        raise NotImplementedError()


def AND(*args: Union[str, QueryObject]):
    return Op(op_and, *args)


def OR(*args: Union[str, QueryObject]):
    return Op(op_or, *args)


t = Term()
