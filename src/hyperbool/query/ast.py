from dataclasses import dataclass
from typing import List


class ASTNode:
    pass


@dataclass
class OperatorAST(ASTNode):
    operator: str
    children: List[ASTNode]


@dataclass
class AtomAST(ASTNode):
    query: str
    field: List[str]
