import functools
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from inspect import signature
from typing import override, Union, Dict, List

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


class QPP(ABC):
    def measure(self, index: engine.Indexer, query: Union[Q, ASTNode], identifier: str) -> QPPResult:
        return QPPResult(self.name, identifier, self._measure(index, query))

    @property
    def name(self) -> str:
        return self.__class__.__name__

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

    @override
    def _measure(self, index: engine.Indexer, query: Q) -> float:
        return index.count(query)


class _NumBooleanClauses(QPP):
    """
    Measure the number of boolean clauses in a query.
    """

    @override
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

    @override
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

    @override
    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def count_mesh_keywords(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return sum([count_mesh_keywords(c) for c in node.children])
            else:
                if isinstance(node, AtomNode):
                    if fields.mapping[node.field][0] in ["mesh_heading_list", "mesh_qualifier_list", "mesh_major_heading_list", "supplementary_concept_list"]:
                        return 1

        return count_mesh_keywords(query)


class _MaximumDepth(QPP):
    """
    Measure the maximum depth of a query.
    """

    @override
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

    @override
    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def max_mesh_depth(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return max([max_mesh_depth(c) for c in node.children])
            else:
                if isinstance(node, AtomNode):
                    if fields.mapping[node.field][0] in ["mesh_heading_list", "mesh_qualifier_list", "mesh_major_heading_list", "supplementary_concept_list"]:
                        return 1

        return max_mesh_depth(query)


class _AverageMeSHDepth(QPP):
    """
    Measure the maximum depth of a query.
    """

    @override
    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def avg_mesh_depth(node: ASTNode) -> float:
            if isinstance(node, OperatorNode):
                return sum([avg_mesh_depth(c) for c in node.children]) / len(node.children)
            else:
                if isinstance(node, AtomNode):
                    if fields.mapping[node.field][0] in ["mesh_heading_list", "mesh_qualifier_list", "mesh_major_heading_list", "supplementary_concept_list"]:
                        return 1

        return avg_mesh_depth(query)


class _MaximumWidth(QPP):
    """
    Measure the maximum width of a query.
    """

    @override
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

    @override
    def _measure(self, index: engine.Indexer, query: ASTNode) -> float:
        def root_width(node: ASTNode) -> int:
            if isinstance(node, OperatorNode):
                return len(node.children)
            else:
                return 1

        return root_width(query)


NumRetrieved = _NumRetrieved()
NumBooleanClauses = _NumBooleanClauses()
NumKeywords = _NumKeywords()
NumMeSHKeywords = _NumMeSHKeywords()
MaximumDepth = _MaximumDepth()
MaximumMeSHDepth = _MaximumMeSHDepth()
AverageMeSHDepth = _AverageMeSHDepth()
MaximumWidth = _MaximumWidth()
RootWidth = _RootWidth()


class QPPExperiment(RetrievalExperiment):

    @property
    @functools.cache
    def ast_queries(self) -> Dict[str, ASTNode]:
        d = {}
        for topic in self.collection.topics:
            ast_query = self.query_parser.parse_ast(topic.raw_query)
            d[topic.identifier] = ast_query
        return d

    @override
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
