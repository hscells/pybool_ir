from pathlib import Path
from typing import Union, Iterable, List

import pandas as pd

from pybool_ir.index import Indexer
from pybool_ir.index.document import Document
from pybool_ir.query.lqd import lqd_parse
from pybool_ir.query.lqd.parser import MetaFn, Value, LqdList, LqdDict, LqdInt
from pybool_ir.query.units import Q


class Environment(Indexer):
    def __init__(self, index_path: Union[Path, str]):
        super().__init__(index_path)
        self.stack: List[Value] = []

    def eval(self, raw_query: str) -> Value:
        expression, ok = lqd_parse(raw_query)
        if not ok:
            raise expression

        val = None
        for e in expression.expression_list:
            if isinstance(e, MetaFn):
                if e.name in fn_map:
                    val = fn_map[e.name](self)
                    if val is not None:
                        self.stack.append(val)
            elif isinstance(e, Value):
                val = e
                self.stack.append(e)
            else:
                raise NotImplementedError(f"Unknown expression: {e}")
        return val

    # -------------------------------------------------------------------------
    # Abstract methods that don't need to be implemented.

    def process_document(self, doc: Document) -> Document:
        pass

    def parse_documents(self, fname: Path) -> (Iterable[Document], int):
        pass

    def set_index_fields(self, store_fields: bool = False):
        pass


def _doc(env: Environment):
    if len(env.stack) == 0:
        return None
    doc_id = env.stack.pop()
    if not isinstance(doc_id, Value) or not doc_id.is_str():
        return None
    hits = env.index.indexSearcher.search(query=Q.regexp("id", doc_id.value))
    if len(hits) > 0:
        return LqdDict(hits[0])
    return None


def _fields(env: Environment):
    d = _doc(env)
    if d is None:
        return None
    return LqdList(list(d.value.keys()))


def _ith(env: Environment):
    if len(env.stack) < 2:
        return None
    i = env.stack.pop()
    l = env.stack.pop()
    if not isinstance(i, Value) or not isinstance(l, Value) or not i.is_int() or not l.is_list():
        return None
    return l.value[i.value]


def _select(env: Environment):
    if len(env.stack) < 2:
        return None

    # Get the keys.
    # Read from the stack until we hit a non-string.
    keys = []
    while len(env.stack) > 0:
        k = env.stack.pop()
        if not isinstance(k, Value) or not k.is_str():
            env.stack.append(k)
            break
        keys.append(k.value)

    d = env.stack.pop()
    if not isinstance(d, Value) or not d.is_dict():
        return None

    return LqdDict({k: d.value[k] for k in keys})


def _ps(env: Environment):
    for i, v in enumerate(env.stack):
        print(f"[{i}]{v} ({v.value})")


def _quit(env: Environment):
    exit(0)


fn_map = {
    # Lucene functions.
    ".fields": _fields,
    ".doc": _doc,

    # List functions.
    ".ith": _ith,

    # Dict functions.
    ".select": _select,

    # Stack functions.
    ".ps": _ps,
    ".clear": lambda env: env.stack.clear(),

    # Misc.
    ".quit": quit
}
