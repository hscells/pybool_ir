"""
Classes and methods for running experiments that involve execution of each atomic node in a query.

An example of what is possible with this module:
>>> from pybool_ir.experiments.decompose import AdHocDecomposeRetrievalExperiment
>>> from pybool_ir.index.pubmed import PubmedIndexer
>>> from pybool_ir.query.ast import AtomNode
>>> def print_callback(node: AtomNode, hits: Hits):
...     print(f"Query: {node.query}, Hits: {hits.length()}")
>>> with AdHocDecomposeRetrievalExperiment(PubmedIndexer(index_path="pubmed"),
...                                        raw_query="(a AND b) OR (c AND d)",
...                                        atomic_callback=print_callback) as exp:
...     exp.go()
a 1000
b 500
c 10
d 1000

"""
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Callable

import appdirs
import lucene
from ir_measures import ScoredDoc, Measure
from lupyne import engine
from lupyne.engine.documents import Hits
from tqdm import tqdm

from pybool_ir.experiments.collections import Topic, Collection
from pybool_ir.experiments.retrieval import RetrievalExperiment
from pybool_ir.index import Indexer
from pybool_ir.query.ast import ASTNode, OperatorNode, AtomNode
from pybool_ir.query.parser import QueryParser
from pybool_ir.query.pubmed.parser import PubmedQueryParser

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query


class Operator(ABC):
    """
     An operator combines document sets together. It is used by the `DecomposeRetrievalExperiment` class to
        combine the results of the subqueries.
     """

    @abstractmethod
    def evaluate(self, child_hits: List[List[int]], child_queries: List[ASTNode]) -> List[int]:
        raise NotImplementedError


# --------------------------------------

class AND(Operator):
    def evaluate(self, child_hits: List[List[int]], child_queries: List[ASTNode]) -> List[int]:
        return list(set(child_hits[0]).intersection(*child_hits[1:]))


class OR(Operator):
    def evaluate(self, child_hits: List[List[int]], child_queries: List[ASTNode]) -> List[int]:
        return list(set().union(*child_hits))


class NOT(Operator):
    def evaluate(self, child_hits: List[List[int]], child_queries: List[ASTNode]) -> List[int]:
        assert len(child_hits) == 2
        return list(set(child_hits[0]).difference(*child_hits[1:]))


# --------------------------------------
class DecomposeRetrievalExperiment(RetrievalExperiment):
    """
    Base class for running experiments that involve execution of each atomic node in a query.

    The `atomic_callback` argument can be used to specify a callback function that is called after each atomic query is executed.
    This is useful for logging the results of each atomic query, or for performing some other operation on the results.
    """

    def __init__(self, indexer: Indexer, collection: Collection,
                 query_parser: QueryParser = PubmedQueryParser(),
                 eval_measures: List[Measure] = None,
                 run_path: Path = None, filter_topics: List[str] = None,
                 ignore_dates: bool = False, date_field: str = "dp", atomic_callback=Callable[[AtomNode, Hits], None]):
        super().__init__(indexer, collection, query_parser, eval_measures, run_path, filter_topics, ignore_dates, date_field)
        #: The callback function that is called after each atomic query is executed.
        self.atomic_callback = atomic_callback
        #: The operators used to combine the results of the atomic queries.
        self.operators = {
            "AND": AND(),
            "OR": OR(),
            "NOT": NOT()
        }
        self._topics = {}
        for t in self.collection.topics:
            self._topics[t.identifier] = t

    @property
    def queries(self) -> Dict[str, ASTNode]:
        """
        This method overrides the default behavior of the `pybool_ir.experiments.retrieval.RetrievalExperiment` class which adds the date restrictions
        to the query. This is not necessary for the `DecomposeRetrievalExperiment` class because the date restrictions
        are applied to each atomic clause. This ensures that all the atomic queries are executed on the same date range.
        """
        return dict([(topic.identifier, self._parsed_queries[i])
                     for i, topic in enumerate(self.collection._topics)])

    def _parse_queries_process(self, t: Topic):
        if len(t.raw_query) > 0:
            return t, self.query_parser.parse_ast(t.raw_query)
        return None, None

    def _query_atom(self, node: AtomNode, query_id) -> List[int]:
        # Apply dates here.
        if not self.ignore_dates:
            topic = self._topics[query_id]
            node = OperatorNode("AND", [node, AtomNode(f"{topic.date_from}:{topic.date_to}", self.date_field)])
        lucene_query = self.query_parser.transform(node)
        count = None

        hits = list(self.index.search(lucene_query, count=count).ids)
        self.atomic_callback(node, hits)
        return hits

    def _query_operator(self, node: OperatorNode, query_id) -> List[int]:
        child_hits = []
        for i, child in enumerate(node.children):
            child_hits.append(list(self._hits_recurse(child, query_id)))
        results = self.operators[node.operator.upper()].evaluate(child_hits, node.children)
        return results

    def _hits_recurse(self, node, query_id) -> List[int | str]:
        if isinstance(node, AtomNode):
            # Borked parsing issue.
            if not isinstance(node.field, str):
                node.field = node.field.field
            return self._query_atom(node, query_id)
        elif isinstance(node, OperatorNode):
            return self._query_operator(node, query_id)

    def _retrieval(self) -> List[ScoredDoc]:
        for query_id, ast_node in tqdm(self.queries.items(), desc="retrieval"):
            assert isinstance(ast_node, OperatorNode)
            lucene_ids = self._hits_recurse(ast_node, query_id)
            for i, lucene_id in enumerate(lucene_ids):
                doc = self.index.get(lucene_id, "id")
                yield ScoredDoc(query_id, doc["id"], len(lucene_ids) - i)
        self.date_completed = datetime.now()


def AdHocDecomposeRetrievalExperiment(indexer: Indexer, raw_query: str = None,
                                      query_parser: QueryParser = PubmedQueryParser(),
                                      date_from="1900/01/01", date_to="3000/01/01",
                                      ignore_dates: bool = False, date_field: str = "dp",
                                      atomic_callback=Callable[[AtomNode, Hits], None]) -> DecomposeRetrievalExperiment:
    """
    Create an experiment that decomposes a query into atomic nodes and executes each one.
    This method can be used to run experiments "on the fly" without having to refer to a collection.
    """
    collection = Collection("adhoc", [], [])
    if raw_query is not None:
        collection = Collection("adhoc", [Topic(identifier="0",
                                                description="ad-hoc topic",
                                                raw_query=raw_query,
                                                date_from=date_from,
                                                date_to=date_to)], [])
    return DecomposeRetrievalExperiment(indexer, collection, query_parser, ignore_dates=ignore_dates,
                                        date_field=date_field, atomic_callback=atomic_callback)
