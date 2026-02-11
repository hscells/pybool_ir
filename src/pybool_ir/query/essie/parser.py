"""
Implementation of Essie query parser.
"""

import datetime
from abc import abstractmethod
from calendar import monthrange
from copy import deepcopy
from typing import List

import lucene
from lupyne import engine
from lupyne.engine import DateTimeField
# noinspection PyUnresolvedReferences
from org.apache.lucene import search
from pyparsing import (
    Word,
    Optional,
    alphanums,
    Forward,
    CaselessKeyword,
    Suppress, infix_notation, OpAssoc, Group, Literal, Combine, OneOrMore, nums, White, PrecededBy)

from pybool_ir.query.parser import QueryParser
from pybool_ir.query.ast import OperatorNode, AtomNode, ASTNode
from pybool_ir.query.units import UnitAtom, QueryAtom

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
analyzer = engine.analyzers.Analyzer.standard()


_all_fields = ["org_study_id",
                "secondary_id",
                "nct_id",
                "brief_title",
                "official_title",
                "sponsors",
                "source",
                "brief_summary",
                "detailed_description",
                "overall_status",
                "phase",
                "study_type",
                "has_expanded_access",
                "primary_outcome",
                "secondary_outcomes",
                "condition",
                "intervention",
                "criteria",
                "gender",
                "minimum_age",
                "maximum_age",
                "healthy_volunteers",
                "location",
                "location_countries",
                "verification_date",
                "study_first_submitted",
                "study_first_submitted_qc",
                "study_first_posted",
                "last_update_submitted",
                "late_update_submitted_qc",
                "last_update_posted",
                "keyword",
                "intervention_browse"
                "condition_browse"]

# --------------------------------------
# Explanation of first three classes here:
# https://stackoverflow.com/a/47186319

# You can call the .__query__() method to create a Lucene query.
# You can call the .__ast__() method to create an AST object.
class _ParseNode(object):
    @abstractmethod
    def __query__(self, optional_fields: List[str] = None):
        raise NotImplementedError()

    @abstractmethod
    def __ast__(self):
        raise NotImplementedError()


class _OpNode:
    def __init__(self, tokens):
        self.operator = tokens[0][1]
        self.operands = tokens[0][::2]

    def __repr__(self):
        return "({}<{}>:{!r})".format(self.__class__.__name__,
                                      self.operator, self.operands)

    def __ast__(self):
        return OperatorNode(operator=self.operator, children=[node.__ast__() for node in self.operands])


class _NotOp(_OpNode, _ParseNode):
    def __query__(self, optional_fields: List[str] = None):
        q = self.operands[0].__query__(optional_fields=optional_fields)
        builder = search.BooleanQuery.Builder()
        builder.add(q, search.BooleanClause.Occur.MUST_NOT)
        return builder.build()


class _BinOp(_OpNode, _ParseNode):
    def __query__(self, optional_fields: List[str] = None):
        if self.operator == "AND":
            op = Q.all
        else:
            op = Q.any
        return op(*[operand.__query__(optional_fields=optional_fields) for operand in self.operands])


class _Atom(_ParseNode):
    def __init__(self, tokens):
        self.unit: UnitAtom = tokens[0][0]
        self.field = None

    def __repr__(self):
        return f"{self.unit}"

    def __ast__(self):
        return AtomNode(query=self.unit.raw_query, field=self.field)

    def __query__(self, optional_fields: List[str] = None):
        expansion_atoms = []

        # Phrases.
        if isinstance(self.unit, QueryAtom):
            return Q.any(*[Q.term(f, self.unit.analyzed_query) for f in _all_fields])


class _DateAtom(UnitAtom):
    def __init__(self, tokens):
        self.month = None
        self.day = None
        self.year = int(tokens[0])
        self._query = repr(self)

        if len(tokens) > 1:
            self.month = int(tokens[1])

        if len(tokens) > 2:
            self.day = int(tokens[2])

    @property
    def query(self):
        return self._query

    @property
    def raw_query(self):
        if self.day is not None:
            return f"{self.year}/{self.month}/{self.day}"
        if self.month is not None:
            return f"{self.year}/{self.month}"
        return f"{self.year}"

    @classmethod
    def from_str(cls, s: str) -> "_DateAtom":
        return cls(s.split("/"))

    def __repr__(self):
        return self.raw_query

class _DateRangeAtom(UnitAtom):
    def __init__(self, tokens):
        self.date_from = tokens[0]
        self.date_to = tokens[1]
        self._query = f"{repr(self.date_from)}:{repr(self.date_to)}"

    @property
    def query(self):
        return self._query

    @property
    def raw_query(self):
        return self.__repr__()

    @classmethod
    def from_str(cls, s: str) -> "_DateRangeAtom":
        parts = s.split(":")
        return cls([_DateAtom.from_str(parts[0]), _DateAtom.from_str(parts[1])])

    def __repr__(self):
        return f"{repr(self.date_from)}:{repr(self.date_to)}"


class EssieQueryParser(QueryParser):
    """
    A parser for Essie queries.
    """

    def __init__(self, optional_fields: List[str] = None, optional_operators: List[str] = None):
        super().__init__()
        self.optional_fields = optional_fields
        self.optional_operators = optional_operators

    @classmethod
    def default_field(cls) -> str:
        return "All Fields"

    def _parse(self, raw_query: str) -> _ParseNode:
        # Makes parsing faster. (?)
        # ParserElement.enablePackrat()

        expression = Forward()

        # Boolean operators.
        AND, OR, NOT = map(
            CaselessKeyword, "AND OR NOT".split()
        )

        MISSING = CaselessKeyword("MISSING")
        ALL = CaselessKeyword("ALL")

        # Atoms.
        valid_chars = "αβ-–_,'’&*?."
        valid_quote_chars = valid_chars + "[]/()"
        valid_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + valid_quote_chars + " ") ^ Literal("*")))
        valid_quoteless_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + valid_chars) ^ Literal("*")))

        phrase = Combine(Literal('"') + valid_phrase + Literal('"')).set_parse_action(QueryAtom)
        quoteless_phrase = (Combine(OneOrMore(valid_quoteless_phrase | White(" ", max=1) + ~(White() | AND | OR | NOT)))).set_parse_action(QueryAtom)

        # Atom + Fields.
        atom = Group((quoteless_phrase | phrase)).set_parse_action(_Atom)

        expression << infix_notation(atom, [(NOT, 1, OpAssoc.RIGHT, _NotOp), (OR, 2, OpAssoc.LEFT, _BinOp), (AND, 2, OpAssoc.LEFT, _BinOp)])

        try:
            expression.scan_string(raw_query, debug=True)
        except Exception as e:
            raise e
        try:
            return expression.parse_string(raw_query, parse_all=True)[0]
        except Exception as e:
            raise e

    def parse_lucene(self, raw_query: str) -> Q:
        # NOTE: converting the query to a string makes the date range queries fail. (?)
        try:
            return self._node_to_lucene(self._parse(raw_query))  # .__str__()
        except Exception as e:
            raise e

    def _node_to_lucene(self, node: _ParseNode) -> Q:
        return node.__query__()

    def parse_ast(self, raw_query: str) -> ASTNode:
        return self._parse(raw_query).__ast__()

    def format(self, node: ASTNode) -> str:
        if isinstance(node, AtomNode):
            return f"{node.query}[{node.field}]"
        assert isinstance(node, OperatorNode)
        return f"({f' {node.operator.upper()} '.join([str(x) for x in node.children])})"
