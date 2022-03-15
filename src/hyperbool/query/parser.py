from typing import List

import datetime
from calendar import monthrange
from hyperbool.query import fields

import lucene
from lupyne import engine
from pyparsing import (
    Word,
    Optional,
    alphanums,
    Forward,
    CaselessKeyword,
    ParserElement, Suppress, infix_notation, OpAssoc, Group, WordEnd, Literal, Combine, OneOrMore, FollowedBy, nums)

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
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
        self.operator = tokens[0][0]
        self.operands = [tokens[0][1]]


class BinOp(OpNode):
    def __init__(self, tokens):
        self.operator = tokens[0][1]
        self.operands = tokens[0][::2]

    def __query__(self):
        if self.operator == "AND":
            op = Q.all
        else:
            op = Q.any
        return op(*[operand.__query__() for operand in self.operands])


# The following classes are used to create Lucene queries once parsed.

class Atom():
    def __init__(self, tokens):
        self.query = tokens[0][0]
        self.field = tokens[0][1] if len(tokens[0]) > 1 else default_field

    def __repr__(self):
        return f"{self.query}[{self.field}]"

    def __query__(self):
        mapped_fields = self.field.__query__()
        # Terms.
        if isinstance(self.query, Term):
            query = self.query.__query__()
            if len(mapped_fields) == 1:
                return query(mapped_fields[0], self.query.query)
            return Q.any(*[query(f, self.query.query) for f in mapped_fields])

        # Phrases.
        elif isinstance(self.query, Phrase):
            query = self.query.__query__()
            if len(mapped_fields) == 1:
                return query(mapped_fields[0], *self.query.query)
            return Q.any(*[query(f, *self.query.query) for f in mapped_fields])

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


class Term():
    def __init__(self, tokens):
        self.query = analyzer.parse(tokens[0]).__str__()
        self.fuzzy = tokens[1] if len(tokens) > 1 else ""
        self.query += self.fuzzy

    def __repr__(self):
        return f"Term({self.query})"

    def __query__(self):
        return Q.term


class Phrase():
    def __init__(self, tokens):
        self.query = tokens[0].split()

    def __repr__(self):
        return f"Phrase({self.query})"

    def __query__(self):
        return Q.phrase


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
    def __init__(self, tokens):
        self.field = tokens[0]

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
term = (Word(alphanums + "-_") + Optional(Literal("*"))).set_parse_action(Term)
phrase = (Suppress('"') + Word(alphanums + " -_,") + Suppress('"')).set_parse_action(Phrase)
date = (Word(nums, exact=4) + Optional(Suppress("/") + Word(nums, exact=2) + Optional(Suppress("/") + Word(nums, exact=2)))).set_parse_action(Date)
date_range = (date + Suppress(":") + date).set_parse_action(DateRange)

# Fields.
field_restriction = (Suppress("[") + Word(alphanums + "-_/ ") + Suppress("]")).set_parse_action(Field)

# Atom + Fields.
atom = Group(((date_range | date | term | phrase) + Optional(field_restriction))).set_parse_action(Atom)

# Final expression.
expression = infix_notation(atom, [(NOT, 1, OpAssoc.RIGHT, UnOp), (AND, 2, OpAssoc.LEFT, BinOp), (OR, 2, OpAssoc.LEFT, BinOp)])


def parse_query(query: str) -> Q:
    # NOTE: converting the query to a string makes the date range queries fail. (?)
    return expression.parse_string(query, parse_all=True)[0].__query__()  # .__str__()
