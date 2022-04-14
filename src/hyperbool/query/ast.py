from dataclasses import dataclass
from typing import List


class ASTNode:
    pass


@dataclass
class OperatorNode(ASTNode):
    operator: str
    children: List[ASTNode]

    def __repr__(self):
        return f"({f' {self.operator} '.join([str(x) for x in self.children])})"


@dataclass
class AtomNode(ASTNode):
    query: str
    field: List[str]

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.query}[{self.field}]"
