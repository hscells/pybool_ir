from pyparsing import (
    Word,
    Optional,
    alphanums,
    Forward,
    CaselessKeyword,
    ParserElement, Suppress, infix_notation, OpAssoc)

ParserElement.enablePackrat()
parser = Forward()
AND, OR, NOT = map(
    CaselessKeyword, "AND OR NOT".split()
)

term = Word(alphanums + "-_")
phrase = Suppress('"') + Word(alphanums + " -_") + Suppress('"')
field_restriction = (Suppress("[") + Word(alphanums + "-_/") + Suppress("]")).setName("field_restriction")
atom = ((term | phrase) + Optional(field_restriction)).setName("atom")
expression = infix_notation(atom, [(AND, 2, OpAssoc.LEFT),
                                   (OR, 2, OpAssoc.LEFT),
                                   (NOT, 2, OpAssoc.LEFT)]).setName("expression")
parser <<= expression
