from abc import abstractmethod
from typing import List

import lucene
from lupyne import engine
# noinspection PyUnresolvedReferences
from org.apache.lucene import search
from pyparsing import (
    Word,
    alphanums,
    Forward,
    ParserElement, Literal, Combine, PrecededBy, Group, Suppress, Optional, infix_notation, OpAssoc, CaselessKeyword)

from pybool_ir.query.parser import MAX_CLAUSES
from pybool_ir.query.ast import AtomNode, ASTNode, OperatorNode
from pybool_ir.query.parser import QueryParser
from pybool_ir.query.units import QueryAtom

# ---------------------------------
DEFAULT_FIELD = "content"

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
search.BooleanQuery.setMaxClauseCount(MAX_CLAUSES)  # There is apparently a cap for efficiency reasons.

# Makes parsing faster. (?)
ParserElement.enablePackrat()


class ParseNode(object):
    @abstractmethod
    def __query__(self):
        raise NotImplementedError()

    @abstractmethod
    def __ast__(self):
        raise NotImplementedError()


class Atom(ParseNode):
    def __init__(self, tokens):
        self.unit: QueryAtom = tokens[0][0]
        self.field = tokens[0][1] if len(tokens[0]) > 1 else DEFAULT_FIELD

    def __query__(self):
        if self.unit.quoted:
            return Q.phrase(self.field, self.unit.query)
        return Q.term(self.field, self.unit.raw_query)

    def __ast__(self):
        return AtomNode(query=self.unit.raw_query, field=self.field)

    def __repr__(self):
        return f"{self.unit}[{self.field}]"


class OpNode:
    def __init__(self, tokens):
        self.operator = tokens[0][1]
        self.operands = tokens[0][::2]

    def __repr__(self):
        return "({}<{}>:{!r})".format(self.__class__.__name__,
                                      self.operator, self.operands)

    def __ast__(self):
        return OperatorNode(operator=self.operator, children=[node.__ast__() for node in self.operands])


class NotOp(OpNode, ParseNode):
    def __query__(self):
        lhs = self.operands[0].__query__()
        rhs = self.operands[1].__query__()
        builder = search.BooleanQuery.Builder()
        builder.add(lhs, search.BooleanClause.Occur.SHOULD)
        builder.add(rhs, search.BooleanClause.Occur.MUST_NOT)
        return builder.build()


class BinOp(OpNode, ParseNode):
    def __query__(self):
        if self.operator == "AND":
            op = Q.all
        else:
            op = Q.any
        return op(*[operand.__query__() for operand in self.operands])


class UnsupportedOp(OpNode, ParseNode):
    def __query__(self):
        raise Exception("This query uses a Boolean operator that is not supported in Lucene")


class GenericQueryParser(QueryParser):
    def __init__(self, additional_operators: List[str] = None):
        if additional_operators is None:
            additional_operators = []
        self.additional_operators = additional_operators

    def parse_ast(self, raw_query: str) -> ASTNode:
        return self.parse(raw_query).__ast__()

    def parse_lucene(self, raw_query: str) -> Q:
        try:
            return self.parse(raw_query).__query__()
        except Exception as e:
            print(raw_query)
            raise e

    def parse(self, raw_query: str) -> ParseNode:

        expression = Forward()

        # Boolean operators.
        AND, OR, NOT = map(
            CaselessKeyword, "AND OR NOT".split()
        )

        _valid_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + " ") ^ Literal("*")))
        term = (~PrecededBy(Literal("*")) & (Word(alphanums) ^ Literal("*"))).set_parse_action(QueryAtom)
        phrase = Combine(Literal('"') + _valid_phrase + Literal('"')).set_parse_action(QueryAtom)

        field_restriction = (Suppress(":") + Word(alphanums))

        atom = Group(((term | phrase) + Optional(field_restriction))).set_parse_action(Atom)
        operators = [(NOT, 2, OpAssoc.RIGHT, NotOp), (OR, 2, OpAssoc.LEFT, BinOp), (AND, 2, OpAssoc.LEFT, BinOp)] + \
                    [(CaselessKeyword(x), 2, OpAssoc.LEFT, UnsupportedOp) for x in self.additional_operators]
        expression << infix_notation(atom, operators)

        try:
            expression.scan_string(raw_query, debug=True)
        except Exception as e:
            print(raw_query)
            raise e
        return expression.parse_string(raw_query, parse_all=True)[0]