import datetime
from abc import abstractmethod
from calendar import monthrange
from copy import copy, deepcopy
from typing import List

import lucene
from lupyne import engine
# noinspection PyUnresolvedReferences
from org.apache.lucene import search
from pyparsing import (
    Word,
    Optional,
    alphanums,
    Forward,
    CaselessKeyword,
    ParserElement, Suppress, infix_notation, OpAssoc, Group, Literal, Combine, OneOrMore, nums, White, PrecededBy)

from hyperbool.pubmed.mesh import MeSHTree
from hyperbool.query import fields
from hyperbool.query.ast import OperatorAST, AtomAST, ASTNode

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
search.BooleanQuery.setMaxClauseCount(4096)  # There is apparently a cap for efficiency reasons.
analyzer = engine.analyzers.Analyzer.standard()
indexer = engine.Indexer(directory=None)


# --------------------------------------
# Explanation of first three classes here:
# https://stackoverflow.com/a/47186319

# You can call the .__query__() method to create a Lucene query.
# You can call the .__ast__() method to create an AST object.

class ParseNode(object):
    @abstractmethod
    def __query__(self, tree: MeSHTree):
        raise NotImplementedError()

    @abstractmethod
    def __ast__(self):
        raise NotImplementedError()


class OpNode:
    def __init__(self, tokens):
        self.operator = tokens[0][1]
        self.operands = tokens[0][::2]

    def __repr__(self):
        return "({}<{}>:{!r})".format(self.__class__.__name__,
                                      self.operator, self.operands)

    def __ast__(self):
        return OperatorAST(operator=self.operator, children=[node.__ast__() for node in self.operands])


class NotOp(OpNode, ParseNode):
    def __query__(self, tree: MeSHTree):
        lhs = self.operands[0].__query__(tree)
        rhs = self.operands[1].__query__(tree)
        return Q.boolean(search.BooleanClause.Occur.SHOULD,
                         *[lhs,
                           Q.boolean(search.BooleanClause.Occur.MUST_NOT, rhs)])


class BinOp(OpNode, ParseNode):
    def __query__(self, tree: MeSHTree):
        if self.operator == "AND":
            op = Q.all
        else:
            op = Q.any
        return op(*[operand.__query__(tree) for operand in self.operands])


# The following classes are used to create Lucene queries once parsed.

class Atom(ParseNode):
    def __init__(self, tokens):
        self.unit: UnitAtom = tokens[0][0]
        self.field = tokens[0][1] if len(tokens[0]) > 1 else default_field

    def __repr__(self):
        return f"{self.unit}[{self.field}]"

    def __ast__(self):
        return AtomAST(query=self.unit.query, field=self.field)

    @staticmethod
    def has_mesh_field(mapped_fields: List[str]) -> bool:
        return "mesh_heading_list" in mapped_fields or \
               "mesh_major_heading_list" in mapped_fields or \
               "mesh_qualifier_list" in mapped_fields

    def __query__(self, tree: MeSHTree):
        mapped_fields = deepcopy(self.field.lucene_fields())

        # Perform the subsumption (explosion) of MeSH terms.
        expansion_atoms = []
        if self.field.field_op is None:
            if self.has_mesh_field(mapped_fields):
                for heading in tree.explode(self.unit.query):
                    expansion_atoms.append(Q.regexp("mesh_heading_list", heading))
                mapped_fields.remove("mesh_heading_list")

        # Special case for MeSH query with qualifier.
        if isinstance(self.unit, MeSHAndQualifierAtom):
            lhs = [Q.phrase(f, self.unit.query[0]) for f in mapped_fields]
            rhs = Q.phrase("mesh_qualifier_list", self.unit.query[1])
            return Q.boolean(search.BooleanClause.Occur.MUST,
                             *[
                                 Q.any(*lhs + expansion_atoms),
                                 rhs
                             ])

        # Phrases.
        if isinstance(self.unit, QueryAtom):
            op = Q.phrase
            if self.has_mesh_field(mapped_fields):
                self.unit.quoted = True
                op = Q.regexp

            if not self.unit.quoted:
                if len(mapped_fields) == 1:
                    return Q.any(*[Q.all(*[Q.term(mapped_fields[0], q) for q in self.unit.query.split()])] + expansion_atoms)
                return Q.any(*[Q.all(*[Q.term(f, q) for q in self.unit.query.split()]) for f in mapped_fields] + expansion_atoms)

            if len(mapped_fields) == 1:
                return Q.any(*[op(mapped_fields[0], *self.unit.query.split())] + expansion_atoms)
            return Q.any(*[op(f, *self.unit.query.split()) for f in mapped_fields] + expansion_atoms)

        # Dates.
        elif isinstance(self.unit, DateAtom):
            assert len(mapped_fields) == 1
            field = indexer.set(mapped_fields[0], engine.DateTimeField, stored=True)

            # There is a special case if we have the fully specified date.
            if self.unit.day is not None:
                return field.prefix(datetime.date(self.unit.year, self.unit.month, self.unit.day))

            # Otherwise, we are actually looking at a range.
            elif self.unit.month is not None:
                day_start, day_end = monthrange(self.unit.year, self.unit.month)
                return field.range(datetime.date(self.unit.year, self.unit.month, day_start), datetime.date(self.unit.year, self.unit.month, day_end))
            _, day_end = monthrange(self.unit.year, 12)
            return field.range(datetime.date(self.unit.year, 1, 1), datetime.date(self.unit.year, 12, day_end))

        # Date ranges.
        elif isinstance(self.unit, DateRangeAtom):
            assert len(mapped_fields) == 1
            field = indexer.set(mapped_fields[0], engine.DateTimeField, stored=True)

            # First, create the "from date".
            if self.unit.date_from.day is not None:
                date_from = datetime.date(self.unit.date_from.year, self.unit.date_from.month, self.unit.date_from.day)
            elif self.unit.date_from.month is not None:
                date_from = datetime.date(self.unit.date_from.year, self.unit.date_from.month, 1)
            else:
                date_from = datetime.date(self.unit.date_from.year, 1, 1)

            # Then, create the "to date".
            if self.unit.date_to.day is not None:
                date_to = datetime.date(self.unit.date_to.year, self.unit.date_to.month, self.unit.date_to.day)
            elif self.unit.date_to.month is not None:
                _, day_end = monthrange(self.unit.date_to.year, self.unit.date_to.month)
                date_to = datetime.date(self.unit.date_to.year, self.unit.date_to.month, day_end)
            else:
                _, day_end = monthrange(self.unit.date_to.year, 12)
                date_to = datetime.date(self.unit.date_to.year, 12, day_end)

            # Then, create the range query using the "from date" and "to date".
            return field.range(date_from, date_to)


class UnitAtom(object):
    @property
    @abstractmethod
    def query(self):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def from_str(cls, s: str) -> "UnitAtom":
        raise NotImplementedError()


class QueryAtom(UnitAtom):
    def __init__(self, tokens):
        self._query = analyzer.parse(tokens[0]).__str__()
        self.quoted = True if self.query.startswith('"') and self.query.endswith('"') else False
        self.fuzzy = True if "*" in tokens[0] else False

        if self.quoted:
            self._query = self.query[1:-1]

    @property
    def query(self):
        return self._query

    @classmethod
    def from_str(cls, s: str) -> "QueryAtom":
        return cls([s])

    def __repr__(self):
        return f"Phrase({self.query})"


class MeSHAndQualifierAtom(UnitAtom):
    def __init__(self, tokens):
        self._query = tokens[0]
        self._qualifier = tokens[1]

    @property
    def query(self):
        return self._query, self._qualifier

    @classmethod
    def from_str(cls, s: str) -> "UnitAtom":
        return cls([s])

    def __repr__(self):
        return f"{self._query}/{self._qualifier}"


class DateAtom(UnitAtom):
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

    @classmethod
    def from_str(cls, s: str) -> "DateAtom":
        return cls(s.split("/"))

    def __repr__(self):
        return f"{self.year}/{self.month}/{self.day}"


class DateRangeAtom(UnitAtom):
    def __init__(self, tokens):
        self.date_from = tokens[0]
        self.date_to = tokens[1]
        self._query = f"{repr(self.date_from)}:{repr(self.date_to)}"

    @property
    def query(self):
        return self._query

    @classmethod
    def from_str(cls, s: str) -> "DateRangeAtom":
        parts = s.split(":")
        return cls([DateAtom.from_str(parts[0]), DateAtom.from_str(parts[1])])

    def __repr__(self):
        return f"{repr(self.date_from)}:{repr(self.date_to)}"


class FieldUnit:
    def __init__(self, tokens):
        self.field = tokens[0]
        self.field_op = None
        if len(tokens) > 1:
            self.field_op = tokens[1]

    @classmethod
    def from_str(cls, s: str) -> "FieldUnit":
        parts = s.split(":")
        return cls(parts) if len(parts) > 0 else cls([s])

    def __repr__(self):
        return f"{self.field}"

    def lucene_fields(self):
        return fields.mapping[self.field]


default_field = FieldUnit(["Title/Abstract"])
# --------------------------------------

# Makes parsing faster. (?)
ParserElement.enablePackrat()

expression = Forward()

# Boolean operators.
AND, OR, NOT = map(
    CaselessKeyword, "AND OR NOT".split()
)

# Atoms.
valid_chars = "α-–_,'’"
valid_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + valid_chars + " ") ^ Literal("*")))
valid_quoteless_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + valid_chars) ^ Literal("*")))

phrase = Combine(Literal('"') + valid_phrase + Literal('"')).set_parse_action(QueryAtom)
quoteless_phrase = (Combine(OneOrMore(valid_quoteless_phrase | White(" ", max=1) + ~(White() | AND | OR | NOT)))).set_parse_action(QueryAtom)
mesh_and_qualifier = (Word(alphanums + valid_chars + " ") + Suppress(Literal("/")) + Word(alphanums + valid_chars + " ")).set_parse_action(MeSHAndQualifierAtom)
date = (Word(nums, exact=4) + Optional(Suppress("/") + Word(nums, exact=2) + Optional(Suppress("/") + Word(nums, exact=2)))).set_parse_action(DateAtom)
date_range = (date + Suppress(":") + date).set_parse_action(DateRangeAtom)

# Fields.
field_restriction = (Suppress("[") + Word(alphanums + "-_/ ") + Optional(Literal(":noexp")) + Suppress("]")).set_parse_action(FieldUnit)

# Atom + Fields.
atom = Group((mesh_and_qualifier + field_restriction) | ((date_range | date | quoteless_phrase | phrase) + Optional(field_restriction))).set_parse_action(Atom)

# Final expression.
expression << infix_notation(atom, [(NOT, 2, OpAssoc.RIGHT, NotOp), (OR, 2, OpAssoc.LEFT, BinOp), (AND, 2, OpAssoc.LEFT, BinOp)])


class PubmedQueryParser:
    def __init__(self, tree: MeSHTree = MeSHTree()):
        self.tree = tree

    @staticmethod
    def parse(raw_query: str) -> ParseNode:
        try:
            expression.scan_string(raw_query, debug=True)
        except Exception as e:
            raise e
        return expression.parse_string(raw_query, parse_all=True)[0]

    def parse_lucene(self, raw_query: str) -> Q:
        # NOTE: converting the query to a string makes the date range queries fail. (?)
        return self.node_to_lucene(self.parse(raw_query))  # .__str__()

    def node_to_lucene(self, node: ParseNode) -> Q:
        return node.__query__(tree=self.tree)

    def parse_ast(self, raw_query: str) -> ASTNode:
        return self.parse(raw_query).__ast__()