from typing import List

import datetime
from calendar import monthrange

from hyperbool.pubmed import mesh
from hyperbool.pubmed.mesh import MeSHTree
from hyperbool.query import fields

import lucene
# noinspection PyUnresolvedReferences
from org.apache.lucene import search

from lupyne import engine
from pyparsing import (
    Word,
    Optional,
    alphanums,
    Forward,
    CaselessKeyword,
    ParserElement, Suppress, infix_notation, OpAssoc, Group, WordEnd, Literal, Combine, OneOrMore, FollowedBy, nums, ZeroOrMore, White, printables, OnlyOnce, PrecededBy)

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
search.BooleanQuery.setMaxClauseCount(4096)  # There is apparently a cap for efficiency reasons.
analyzer = engine.analyzers.Analyzer.standard()
indexer = engine.Indexer(directory=None)


# --------------------------------------
# Explanation of first three classes here:
# https://stackoverflow.com/a/47186319

# You can call the .__query__() method to create a Lucene query.

class OpNode:
    def __repr__(self):
        return "{}({}):{!r}".format(self.__class__.__name__,
                                    self.operator, self.operands)


class UnOp(OpNode):
    def __init__(self, tokens):
        self.operator = tokens[0][1]
        self.operands = tokens[0][::2]

    def __query__(self, tree: MeSHTree):
        return Q.boolean(search.BooleanClause.Occur.SHOULD,
                         *[self.operands[0].__query__(tree), Q.boolean(search.BooleanClause.Occur.MUST_NOT,
                                                                       self.operands[1].__query__(tree))])


class BinOp(OpNode):
    def __init__(self, tokens):
        self.operator = tokens[0][1]
        self.operands = tokens[0][::2]

    def __query__(self, tree: MeSHTree):
        if self.operator == "AND":
            op = Q.all
        else:
            op = Q.any
        return op(*[operand.__query__(tree) for operand in self.operands])


# The following classes are used to create Lucene queries once parsed.

class Atom:
    def __init__(self, tokens):
        self.query = tokens[0][0]
        self.field = tokens[0][1] if len(tokens[0]) > 1 else default_field

    def __repr__(self):
        return f"{self.query}[{self.field}]"

    def __query__(self, tree: MeSHTree):
        mapped_fields = self.field.__query__()

        # TODO: would be great to have the ability to add in
        #       custom expansion methods at this point.
        expansion_atoms = []
        if self.field.field_op is None:
            if "mesh_heading_list" in mapped_fields:
                for heading in tree.explode(self.query.query):
                    expansion_atoms.append(Q.phrase("mesh_heading_list", heading))
        # Terms.
        # if isinstance(self.query, Term):
        #     if len(mapped_fields) == 1:
        #         return Q.any(*[Q.term(mapped_fields[0], self.query.query)] + expansion_atoms)
        #     return Q.any(*[Q.term(f, self.query.query) for f in mapped_fields] + expansion_atoms)

        # Phrases.
        if isinstance(self.query, Phrase):
            # print(self.query.fuzzy, self.query.quoted)
            # if self.query.fuzzy:
            #     q = f"{self.query.query}"
            #     if len(mapped_fields) == 1:
            #         return Q.any(*[Q.wildcard(mapped_fields[0], q)] + expansion_atoms)
            #     return Q.any(*[Q.wildcard(f, q) for f in mapped_fields] + expansion_atoms)

            if not self.query.quoted:
                if len(mapped_fields) == 1:
                    return Q.any(*[Q.all(*[Q.term(mapped_fields[0], q) for q in self.query.query.split()])] + expansion_atoms)
                return Q.any(*[Q.all(*[Q.term(f, q) for q in self.query.query.split()]) for f in mapped_fields] + expansion_atoms)

            if len(mapped_fields) == 1:
                return Q.any(*[Q.phrase(mapped_fields[0], *self.query.query.split())] + expansion_atoms)
            return Q.any(*[Q.phrase(f, *self.query.query.split()) for f in mapped_fields] + expansion_atoms)

        # Dates.
        elif isinstance(self.query, Date):
            assert len(mapped_fields) == 1
            field = indexer.set(mapped_fields[0], engine.DateTimeField, stored=True)

            # There is a special case if we have the fully specified date.
            if self.query.day is not None:
                return field.prefix(datetime.date(self.query.year, self.query.month, self.query.day))

            # Otherwise, we are actually looking at a range.
            elif self.query.month is not None:
                day_start, day_end = monthrange(self.query.year, self.query.month)
                return field.range(datetime.date(self.query.year, self.query.month, day_start), datetime.date(self.query.year, self.query.month, day_end))
            _, day_end = monthrange(self.query.year, 12)
            return field.range(datetime.date(self.query.year, 1, 1), datetime.date(self.query.year, 12, day_end))

        # Date ranges.
        elif isinstance(self.query, DateRange):
            assert len(mapped_fields) == 1
            field = indexer.set(mapped_fields[0], engine.DateTimeField, stored=True)

            # First, create the "from date".
            if self.query.date_from.day is not None:
                date_from = datetime.date(self.query.date_from.year, self.query.date_from.month, self.query.date_from.day)
            elif self.query.date_from.month is not None:
                date_from = datetime.date(self.query.date_from.year, self.query.date_from.month, 1)
            else:
                date_from = datetime.date(self.query.date_from.year, 1, 1)

            # Then, create the "to date".
            if self.query.date_to.day is not None:
                date_to = datetime.date(self.query.date_to.year, self.query.date_to.month, self.query.date_to.day)
            elif self.query.date_to.month is not None:
                _, day_end = monthrange(self.query.date_to.year, self.query.date_to.month)
                date_to = datetime.date(self.query.date_to.year, self.query.date_to.month, day_end)
            else:
                _, day_end = monthrange(self.query.date_to.year, 12)
                date_to = datetime.date(self.query.date_to.year, 12, day_end)

            # Then, create the range query using the "from date" and "to date".
            return field.range(date_from, date_to)


# class Term:
#     def __init__(self, tokens):
#         self.query = analyzer.parse("".join(tokens)).__str__()
#         self.fuzzy = True if len(tokens) > 1 and tokens[1] == "*" else False
#
#     def __repr__(self):
#         return f"Term({self.query})"

class Phrase:
    def __init__(self, tokens):
        self.query = analyzer.parse(tokens[0]).__str__()
        self.quoted = True if self.query.startswith('"') and self.query.endswith('"') else False
        self.fuzzy = True if "*" in tokens[0] else False

        if self.quoted:
            self.query = self.query[1:-1]

    def __repr__(self):
        return f"Phrase({self.query})"


class Date():
    def __init__(self, tokens):
        self.month = None
        self.day = None
        self.year = int(tokens[0])

        if len(tokens) > 1:
            self.month = int(tokens[1])

        if len(tokens) > 2:
            self.day = int(tokens[2])

    def __repr__(self):
        return f"Date({self.year}/{self.month}/{self.day})"


class DateRange():
    def __init__(self, tokens):
        self.date_from = tokens[0]
        self.date_to = tokens[1]

    def __repr__(self):
        return f"DateRange({repr(self.date_from)}:{repr(self.date_to)})"


class Field():
    def __init__(self, tokens, **kwargs):
        self.field = tokens[0]
        self.field_op = None
        if len(tokens) > 1:
            self.field_op = tokens[1]

    def __repr__(self):
        return f"Fields({self.field})"

    def __query__(self):
        return fields.mapping[self.field]


default_field = Field(["Title/Abstract"])
# --------------------------------------

# Makes parsing faster. (?)
ParserElement.enablePackrat()

expression = Forward()

# Boolean operators.
AND, OR, NOT = map(
    CaselessKeyword, "AND OR NOT".split()
)

# Atoms.
# term = (Suppress(Optional(Literal("*"))) + Word(alphanums + "α-–_") + Optional(Literal("*"))).set_parse_action(Term)
valid_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + " α-–_,/'’") ^ Literal("*")))
valid_quoteless_phrase = (~PrecededBy(Literal("*")) & (Word(alphanums + "α-–_,/'’") ^ Literal("*")))
phrase = Combine(Literal('"') + valid_phrase + Literal('"')).set_parse_action(Phrase)
quoteless_phrase = (Combine(OneOrMore(valid_quoteless_phrase | White(" ", max=1) + ~(White() | AND | OR | NOT)))).set_parse_action(Phrase)
date = (Word(nums, exact=4) + Optional(Suppress("/") + Word(nums, exact=2) + Optional(Suppress("/") + Word(nums, exact=2)))).set_parse_action(Date)
date_range = (date + Suppress(":") + date).set_parse_action(DateRange)

# Fields.
field_restriction = (Suppress("[") + Word(alphanums + "-_/ ") + Optional(Literal(":noexp")) + Suppress("]")).set_parse_action(Field)

# Atom + Fields.
atom = Group(((date_range | date | quoteless_phrase | phrase) + Optional(field_restriction))).set_parse_action(Atom)
# atom = Group(((quoteless_phrase) + Optional(field_restriction))).set_parse_action(Atom)

# Final expression.
expression << infix_notation(atom, [(NOT, 2, OpAssoc.LEFT, UnOp), (AND, 2, OpAssoc.LEFT, BinOp), (OR, 2, OpAssoc.LEFT, BinOp)])


def parse_query(query: str, tree: MeSHTree) -> Q:
    try:
        expression.scan_string(query, debug=True)
    except Exception as e:
        return e
    # NOTE: converting the query to a string makes the date range queries fail. (?)
    return expression.parse_string(query, parse_all=True)[0].__query__(tree=tree)  # .__str__()


class PubmedQueryParser:
    def __init__(self, tree: MeSHTree = MeSHTree()):
        self.tree = tree

    def parse(self, raw_query: str) -> Q:
        return parse_query(raw_query, self.tree)
