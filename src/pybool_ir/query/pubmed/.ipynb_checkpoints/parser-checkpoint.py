"""
Implementation of a PubMed query parser.
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

from pybool_ir.datasets.pubmed.mesh import MeSHTree
from pybool_ir.query.parser import MAX_CLAUSES
from pybool_ir.query.parser import QueryParser
from pybool_ir.query.ast import OperatorNode, AtomNode, ASTNode
from pybool_ir.query.pubmed import fields
from pybool_ir.query.units import UnitAtom, QueryAtom

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
search.BooleanQuery.setMaxClauseCount(MAX_CLAUSES)  # There is apparently a cap for efficiency reasons.
analyzer = engine.analyzers.Analyzer.standard()


# --------------------------------------
# Explanation of first three classes here:
# https://stackoverflow.com/a/47186319

# You can call the .__query__() method to create a Lucene query.
# You can call the .__ast__() method to create an AST object.

class _ParseNode(object):
    @abstractmethod
    def __query__(self, tree: MeSHTree, optional_fields: List[str] = None):
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
    def __query__(self, tree: MeSHTree, optional_fields: List[str] = None):
        lhs = self.operands[0].__query__(tree, optional_fields=optional_fields)
        rhs = self.operands[1].__query__(tree, optional_fields=optional_fields)
        builder = search.BooleanQuery.Builder()
        builder.add(lhs, search.BooleanClause.Occur.SHOULD)
        builder.add(rhs, search.BooleanClause.Occur.MUST_NOT)
        return builder.build()


class _BinOp(_OpNode, _ParseNode):
    def __query__(self, tree: MeSHTree, optional_fields: List[str] = None):
        if self.operator == "AND":
            op = Q.all
        else:
            op = Q.any
        return op(*[operand.__query__(tree, optional_fields=optional_fields) for operand in self.operands])


# The following classes are used to create Lucene queries once parsed.

class _Atom(_ParseNode):
    def __init__(self, tokens):
        self.unit: UnitAtom = tokens[0][0]
        self.field = tokens[0][1] if len(tokens[0]) > 1 else _default_field

    def __repr__(self):
        return f"{self.unit}[{self.field}]"

    def __ast__(self):
        return AtomNode(query=self.unit.raw_query, field=self.field)

    @staticmethod
    def has_mesh_field(mapped_fields: List[str]) -> bool:
        return "mesh_heading_list" in mapped_fields or \
            "mesh_major_heading_list" in mapped_fields or \
            "mesh_qualifier_list" in mapped_fields

    def __query__(self, tree: MeSHTree, optional_fields: List[str] = None):
        if optional_fields is not None and self.field.__repr__() in optional_fields:
            mapped_fields = [self.field.__repr__()]
        else:
            mapped_fields = deepcopy(self.field.lucene_fields())
        expansion_atoms = []

        # Special field that is not actually indexed.
        if mapped_fields[0] == "all_fields":
            headings = [x for x in list(tree.locations.keys()) if self.unit.query.lower() in x]
            for heading in headings:
                for exploded_heading in tree.explode(heading):
                    expansion_atoms.append(Q.regexp("mesh_heading_list", exploded_heading))
                    expansion_atoms.append(Q.regexp("publication_type", exploded_heading))
            if " " in self.unit.analyzed_query:
                expansion_atoms += [Q.near("title", *self.unit.analyzed_query.split()),
                                    Q.near("abstract", *self.unit.analyzed_query.split())]
            return Q.any(
                *[
                     Q.wildcard("title", self.unit.analyzed_query),
                     Q.wildcard("abstract", self.unit.analyzed_query),
                     Q.phrase("title", self.unit.analyzed_query),
                     Q.phrase("abstract", self.unit.analyzed_query),
                     Q.term("title", self.unit.analyzed_query),
                     Q.term("abstract", self.unit.analyzed_query)
                 ] + expansion_atoms
            )

        # Special case for MeSH query with qualifier.
        if isinstance(self.unit, _MeSHAndQualifierAtom):
            if self.field.field_op is None:
                for heading in tree.explode(self.unit.query[0]):
                    expansion_atoms.append(Q.regexp("mesh_heading_list", heading))

            lhs = [Q.phrase(f, self.unit.query[0]) for f in mapped_fields]
            rhs = Q.phrase("mesh_qualifier_list", self.unit.query[1])
            return Q.boolean(search.BooleanClause.Occur.MUST,
                             *[
                                 Q.any(*lhs + expansion_atoms),
                                 rhs
                             ])

        # Perform the subsumption (explosion) of MeSH terms.
        if self.has_mesh_field(mapped_fields):
            if mapped_fields[0] == "mesh_qualifier_list":
                return Q.term(mapped_fields[0], self.unit.query.lower().replace(" and ", " & "))
            if self.field.field_op is None:
                for heading in tree.explode(self.unit.query):
                    expansion_atoms.append(Q.regexp("mesh_heading_list", heading))
                expansion_atoms = expansion_atoms[1:]
                expansion_atoms.append(Q.regexp(mapped_fields[0], tree.map_heading(self.unit.query)))
                return Q.any(*expansion_atoms)
            else:
                return Q.regexp(mapped_fields[0], tree.map_heading(self.unit.query))

        if "publication_type" in mapped_fields:
            if self.field.field_op is None:
                for heading in tree.explode(self.unit.query):
                    expansion_atoms.append(Q.regexp("publication_type", heading))
                return Q.any(*expansion_atoms)
            else:
                return Q.regexp("publication_type", tree.map_heading(self.unit.query))

        if "supplementary_concept_list" in mapped_fields:
            return Q.any(*[Q.regexp("supplementary_concept_list", self.unit.query.lower()),
                           Q.regexp("supplementary_concept_list", self.unit.query)])

        # Phrases.
        if isinstance(self.unit, QueryAtom):
            if " " not in self.unit.analyzed_query:
                op = Q.term
                if self.unit.fuzzy:
                    op = Q.wildcard
                if len(mapped_fields) == 1:
                    return op(mapped_fields[0], self.unit.analyzed_query)
                return Q.any(*[op(f, self.unit.analyzed_query) for f in mapped_fields])
            if len(mapped_fields) == 1:
                return Q.near(mapped_fields[0], *self.unit.analyzed_query.split())
            return Q.any(*[Q.near(f, *self.unit.analyzed_query.split()) for f in mapped_fields])

        # Dates.
        elif isinstance(self.unit, _DateAtom):
            assert len(mapped_fields) == 1
            # field = indexer.set(mapped_fields[0], engine.DateTimeField, stored=True)
            field = DateTimeField(mapped_fields[0], stored=True)

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
        elif isinstance(self.unit, _DateRangeAtom):
            assert len(mapped_fields) == 1
            # field = indexer.set(mapped_fields[0], engine.DateTimeField, stored=True)
            field = DateTimeField(mapped_fields[0], stored=True)

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


class _MeSHAndQualifierAtom(UnitAtom):
    def __init__(self, tokens):
        self._query = tokens[0]
        self._qualifier = tokens[1]

    @property
    def query(self):
        return self._query, self._qualifier

    @property
    def raw_query(self):
        return f"{self._query}/{self._qualifier}"

    @classmethod
    def from_str(cls, s: str) -> "UnitAtom":
        return cls([s])

    def __repr__(self):
        return f"{self._query}/{self._qualifier}"


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


class _FieldUnit:
    def __init__(self, tokens):
        self.field = tokens[0]
        self.field_op = None
        if len(tokens) > 1:
            self.field_op = tokens[1]

    @classmethod
    def from_str(cls, s: str) -> "_FieldUnit":
        parts = s.split(":")
        return cls(parts) if len(parts) > 0 else cls([s])

    def __repr__(self):
        if self.field_op is not None:
            return f"{self.field}{self.field_op}"
        return f"{self.field}"

    def lucene_fields(self):
        try:
            return fields.mapping[self.field]
        except KeyError:
            raise ValueError(f"Field {self.field} is not a valid field.")


# --------------------------------------


class PubmedQueryParser(QueryParser):
    """
    A parser for Pubmed queries.
    """

    def __init__(self, tree: MeSHTree = MeSHTree(), optional_fields: List[str] = None, optional_operators: List[str] = None):
        super().__init__()
        self.tree = tree
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

        # Atoms.
        valid_chars = "αβ-–_,'’&*?."
        valid_quote_chars = valid_chars + "[]/()"
        valid_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + valid_quote_chars + " ") ^ Literal("*")))
        valid_quoteless_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + valid_chars) ^ Literal("*")))

        phrase = Combine(Literal('"') + valid_phrase + Literal('"')).set_parse_action(QueryAtom)
        quoteless_phrase = (Combine(OneOrMore(valid_quoteless_phrase | White(" ", max=1) + ~(White() | AND | OR | NOT)))).set_parse_action(QueryAtom)
        mesh_and_qualifier = (Suppress(Optional(Literal('"'))) + (Word(alphanums + valid_chars + " ") + Suppress(Literal("/")) + Word(alphanums + valid_chars + " ")) + Suppress(Optional(Literal('"')))).set_parse_action(_MeSHAndQualifierAtom)
        date = (Word(nums, exact=4) + Optional(Suppress("/") + Word(nums, exact=2) + Optional(Suppress("/") + Word(nums, exact=2)))).set_parse_action(_DateAtom)
        date_range = (date + Suppress(":") + date).set_parse_action(_DateRangeAtom)

        # Fields.
        field_restriction = (Suppress("[") + Word(alphanums + "-_/ ") + Optional(Literal(":noexp")) + Suppress("]")).set_parse_action(_FieldUnit)

        # Atom + Fields.
        atom = Group((mesh_and_qualifier + field_restriction) | ((date_range | date | quoteless_phrase | phrase) + Optional(field_restriction))).set_parse_action(_Atom)

        # Final expression.
        optional_operators = []
        if self.optional_operators is not None:
            optional_operators = [(CaselessKeyword(op), 2, OpAssoc.LEFT, _BinOp) for op in self.optional_operators]
        expression << infix_notation(atom, [(NOT, 2, OpAssoc.RIGHT, _NotOp), (OR, 2, OpAssoc.LEFT, _BinOp), (AND, 2, OpAssoc.LEFT, _BinOp)] + optional_operators)

        raw_query = raw_query.replace(":NoExp", ":noexp")
        try:
            expression.scan_string(raw_query, debug=True)
        except Exception as e:
            print(raw_query)
            raise e
        try:
            return expression.parse_string(raw_query, parse_all=True)[0]
        except Exception as e:
            print(raw_query)
            raise e

    def parse_lucene(self, raw_query: str) -> Q:
        # NOTE: converting the query to a string makes the date range queries fail. (?)
        try:
            return self._node_to_lucene(self._parse(raw_query))  # .__str__()
        except Exception as e:
            print(raw_query)
            raise e

    def _node_to_lucene(self, node: _ParseNode) -> Q:
        return node.__query__(tree=self.tree, optional_fields=self.optional_fields)

    def parse_ast(self, raw_query: str) -> ASTNode:
        return self._parse(raw_query).__ast__()

    def format(self, node: ASTNode) -> str:
        if isinstance(node, AtomNode):
            return f"{node.query}[{node.field}]"
        assert isinstance(node, OperatorNode)
        return f"({f' {node.operator.upper()} '.join([str(x) for x in node.children])})"


# --------------------------------------
#: The name of the default field that is used when no field is specified.
_default_field = _FieldUnit([PubmedQueryParser.default_field()])
