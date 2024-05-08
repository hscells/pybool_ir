import functools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from inspect import signature
from typing import Union, Dict, List, Callable, Optional

from dataclasses_json import dataclass_json
from lupyne import engine

from pybool_ir.experiments.collections import Topic, Collection
from pybool_ir.experiments.retrieval import RetrievalExperiment
from pybool_ir.index import Indexer
from pybool_ir.query import PubmedQueryParser
from pybool_ir.query.parser import QueryParser
from pybool_ir.query.pubmed import fields
from pybool_ir.query.ast import ASTNode, OperatorNode, AtomNode

Q = engine.Query


class QPPType(Enum):
    lucene = 0
    ast = 1


@dataclass
@dataclass_json
class QPPResult:
    qpp: str
    query: str
    result: float

    def __init__(self, qpp: str, query: str, result: float):
        self.qpp = qpp
        self.query = query
        self.result = result


class QPP(ABC):
    def measure(self, index: engine.Indexer, query: Union[Q, ASTNode], identifier: str) -> QPPResult:
        return QPPResult(self.name, identifier, self._measure(index, query))

    @property
    def name(self) -> str:
        return self.__class__.__name__.replace("_", "")

    @abstractmethod
    def _measure(self, index: engine.Indexer, query: Union[Q, ASTNode]) -> float:
        pass

    def type(self) -> QPPType:
        t = signature(self._measure).parameters['query'].annotation
        if t is Q:
            return QPPType.lucene
        elif t is ASTNode:
            return QPPType.ast
        raise ValueError(f"Unknown query type: {t}")


class _NumRetrieved(QPP):
    """
    Measure the number of documents retrieved by a query.
    """

    def _measure(self, index: engine.Indexer, query: Q) -> float:
        return index.count(query)


class _NumBooleanClauses(QPP):
    """
    Measure the number of boolean clauses in a query.
    """

    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def count_boolean_clauses(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return 1 + sum([count_boolean_clauses(c) for c in node.children])
            else:
                return 0

        return count_boolean_clauses(query)


class _NumKeywords(QPP):
    """
    Measure the number of keywords in a query.
    """

    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def count_keywords(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return sum([count_keywords(c) for c in node.children])
            else:
                return 1

        return count_keywords(query)


class _NumMeSHKeywords(QPP):
    """
    Measure the number of MeSH keywords in a query.
    """

    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def count_mesh_keywords(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return sum([count_mesh_keywords(c) for c in node.children])
            else:
                if isinstance(node, AtomNode):
                    if fields.mapping[node.field.field][0] in ["mesh_heading_list", "mesh_qualifier_list", "mesh_major_heading_list", "supplementary_concept_list"]:
                        return 1
                return 0

        return count_mesh_keywords(query)


class _MaximumDepth(QPP):
    """
    Measure the maximum depth of a query.
    """

    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def max_depth(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return 1 + max([max_depth(c) for c in node.children])
            else:
                return 1

        return max_depth(query)


class _MaximumMeSHDepth(QPP):
    """
    Measure the maximum depth of a query.
    """

    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def max_mesh_depth(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return max([max_mesh_depth(c) for c in node.children])
            else:
                if isinstance(node, AtomNode):
                    if fields.mapping[node.field.field][0] in ["mesh_heading_list", "mesh_qualifier_list", "mesh_major_heading_list", "supplementary_concept_list"]:
                        return 1
                return 0

        return max_mesh_depth(query)


class _AverageMeSHDepth(QPP):
    """
    Measure the maximum depth of a query.
    """

    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def avg_mesh_depth(node: ASTNode) -> float:
            if isinstance(node, OperatorNode):
                return sum([avg_mesh_depth(c) for c in node.children]) / len(node.children)
            else:
                if isinstance(node, AtomNode):
                    if fields.mapping[node.field.field][0] in ["mesh_heading_list", "mesh_qualifier_list", "mesh_major_heading_list", "supplementary_concept_list"]:
                        return 1
                return 0

        return avg_mesh_depth(query)


class _MaximumWidth(QPP):
    """
    Measure the maximum width of a query.
    """

    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def max_width(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return max([max_width(c) for c in node.children])
            else:
                return 1

        return max_width(query)


class _RootWidth(QPP):
    """
    Measure the width of the root of a query.
    """

    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def root_width(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return len(node.children)
            else:
                return 1

        return root_width(query)


def FuncQPP(name: str, func: Callable[[Union[Q, ASTNode], Optional[engine.Indexer]], float]) -> QPP:
    class _QPP(QPP):
        def __init__(self, name, f):
            super().__init__()
            sig = signature(f)
            self._f1 = None
            self._f2 = None
            if sig.parameters['query'].annotation not in [Q, ASTNode]:
                raise ValueError("Function must have a query parameter with engine.Query or ASTNode type hint")
            if sig.parameters["query"].annotation is Q and "index" not in sig.parameters and sig.parameters["index"].annotation is not engine.Indexer:
                raise ValueError("Function must have an index parameter with engine.Indexer type hint")

            if sig.parameters["query"].annotation is Q:
                self._f1 = f
            else:
                self._f2 = f

            self._name = name

        @property
        def name(self) -> str:
            return self._name

        def _measure(self, index: engine.Indexer, query: Q) -> float:
            if self._f1 is not None:
                return self._f1(query, index)
            else:
                return self._f2(query)

    return _QPP(name, func)


NumRetrieved = _NumRetrieved()
NumBooleanClauses = _NumBooleanClauses()
NumKeywords = _NumKeywords()
NumMeSHKeywords = _NumMeSHKeywords()
MaximumDepth = _MaximumDepth()
MaximumMeSHDepth = _MaximumMeSHDepth()
AverageMeSHDepth = _AverageMeSHDepth()
MaximumWidth = _MaximumWidth()
RootWidth = _RootWidth()
AllQPPs = [NumRetrieved,
           NumBooleanClauses,
           NumKeywords,
           NumMeSHKeywords,
           MaximumDepth,
           MaximumMeSHDepth,
           AverageMeSHDepth,
           MaximumWidth,
           RootWidth]


class QPPExperiment(RetrievalExperiment):

    @property
    @functools.cache
    def ast_queries(self) -> Dict[str, ASTNode]:
        d = {}
        for topic in self.collection.topics:
            print(topic.raw_query)
            ast_query = self.query_parser.parse_ast(topic.raw_query)
            d[topic.identifier] = ast_query
        return d

    def results(self, *qpps: QPP) -> List[QPPResult]:
        for qpp in qpps:
            for topic in self.collection.topics:
                match qpp.type():
                    case QPPType.lucene:
                        query = self.queries[topic.identifier]
                    case QPPType.ast:
                        query = self.ast_queries[topic.identifier]
                    case _:
                        raise ValueError("Should never arrive here")
                yield qpp.measure(self.index, query, topic.identifier)


def AdHocQPPExperiment(indexer: Indexer, raw_query: str, topic_id: str = "0",
                       query_parser: QueryParser = PubmedQueryParser(),
                       date_from="1900/01/01", date_to="3000/01/01", ignore_dates: bool = False, date_field: str = "dp") -> QPPExperiment:
    collection = Collection("adhoc", [Topic(identifier=topic_id,
                                            description="ad-hoc topic",
                                            raw_query=raw_query,
                                            date_from=date_from,
                                            date_to=date_to)], [])
    return QPPExperiment(indexer, collection, query_parser, ignore_dates=ignore_dates, date_field=date_field)
