from pyparsing import (
    Word,
    alphanums,
    Forward,
    Literal, Suppress, OneOrMore, nums, Optional)


class MetaFn:
    def __init__(self, tokens):
        self.name = tokens[0] + tokens[1]


class Value:
    def __init__(self, tokens):
        self.value = tokens

    def is_str(self):
        return isinstance(self, LqdStr)

    def is_int(self):
        return isinstance(self, LqdInt)

    def is_list(self):
        return isinstance(self, LqdList)

    def is_dict(self):
        return isinstance(self, LqdDict)

class LqdStr(Value):
    def __init__(self, tokens):
        super().__init__(tokens)


class LqdInt(Value):
    def __init__(self, tokens):
        super().__init__(int(tokens))


class LqdList(Value):
    def __init__(self, tokens):
        super().__init__(tokens)


class LqdDict(Value):
    def __init__(self, tokens):
        super().__init__(tokens)


class Expression:
    def __init__(self, tokens):
        self.expression_list = tokens


expression = Forward()

# Meta operators.
t_str = (Suppress('"') + Word(alphanums + "_-.") + Suppress('"')).set_parse_action(lambda x: LqdStr(x[0]))
t_int = (Word(nums)).set_parse_action(lambda x: LqdInt(x[0]))
meta_fn = (Literal(".") + Word(alphanums)).set_parse_action(MetaFn)

expression <<= OneOrMore(meta_fn | t_str | t_int).set_parse_action(Expression)


def lqd_parse(raw_query: str) -> (object, bool):
    try:
        return expression.parse_string(raw_query, parse_all=True)[0], True
    except Exception as e:
        return e, False
