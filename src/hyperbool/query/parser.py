from typing import List

import lucene
from lupyne import engine
from pyparsing import (
    Word,
    Optional,
    alphanums,
    Forward,
    CaselessKeyword,
    ParserElement, Suppress, infix_notation, OpAssoc, Group, WordEnd, Literal, Combine, OneOrMore, FollowedBy)

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query
analyzer = engine.analyzers.Analyzer.standard()

# --------------------------------------
# https://stackoverflow.com/a/47186319
from hyperbool.query import fields


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


class Atom():
    def __init__(self, tokens):
        self.query = analyzer.parse(tokens[0][0]).__str__()
        self.field = tokens[0][1] if len(tokens[0]) > 1 else "Title/Abstract"

    def __repr__(self):
        return f"{self.query}[{self.field}]"

    def __query__(self):

        # Single term atoms.
        if " " in self.query:
            atom = Q.phrase
            query = self.query.split()
        else:
            atom = Q.term
            query = [self.query]

        mapped_fields = fields.mapping[self.field]
        if len(mapped_fields) == 1:
            return atom(mapped_fields[0], query)
        return Q.any(*[atom(f, *query) for f in mapped_fields])


# --------------------------------------

ParserElement.enablePackrat()
expression = Forward()
AND, OR, NOT = map(
    CaselessKeyword, "AND OR NOT".split()
)

term = (Word(alphanums + "-_") + Optional(Literal("*"))).set_parse_action("".join)
phrase = Suppress('"') + Word(alphanums + " -_,") + Suppress('"')
field_restriction = Suppress("[") + Word(alphanums + "-_/") + Suppress("]")
atom = Group(((term | phrase).set_results_name("query") + Optional(field_restriction).set_results_name("field"))).set_results_name("atom", list_all_matches=True).set_parse_action(Atom)
expression = infix_notation(atom, [(AND, 2, OpAssoc.LEFT, BinOp),
                                   (OR, 2, OpAssoc.LEFT, BinOp),
                                   (NOT, 1, OpAssoc.RIGHT, UnOp)]).set_results_name("expression")


def parse_query(query: str) -> Q:
    return expression.parse_string(query, parse_all=True)[0].__query__().__str__()

#
# print(expression.parse_string("pani*[Title]"))
