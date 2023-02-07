"""
Base classes for representing queries.

The `OperatorNode` class represents a query that is a combination of other queries. It can be used to represent a query that is a combination of other queries, such as a Boolean query.

The `AtomNode` class represents a query that is a single query. It can be used to represent a query, such as a term query.
"""

from dataclasses import dataclass
from typing import List


class ASTNode:
    pass


@dataclass
class OperatorNode(ASTNode):
    """
    Represents a query that is a combination of other queries.
    """
    #: The relationship expressed between the children, e.g., AND, OR, NOT.
    operator: str
    #: The children of the node.
    children: List[ASTNode]

    def __repr__(self):
        return f"({f' {self.operator.upper()} '.join([str(x) for x in self.children])})"


@dataclass
class AtomNode(ASTNode):
    """
    Represents a query that is a single query.
    """
    #: The actual string of the query.
    query: str
    #: The field or fields that the query is applied to in the index.
    field: List[str] | str  # TODO: field weighting?

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"{self.query}[{self.field}]"
