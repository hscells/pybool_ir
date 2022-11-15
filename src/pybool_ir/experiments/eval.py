from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict

from ir_measures import ScoredDoc
from tqdm import tqdm
from typing_extensions import override

from pybool_ir.experiments.collections import Topic
from pybool_ir.experiments.retrieval import RetrievalExperiment
from pybool_ir.query.ast import ASTNode, OperatorNode, AtomNode


class Operator(ABC):
    @abstractmethod
    def evaluate(self, child_hits: List[List[str]], child_queries: List[ASTNode]):
        raise NotImplementedError


class AND(Operator):
    def evaluate(self, child_hits: List[List[str]], child_queries: List[ASTNode]):
        return list(set().intersection(*child_hits))


class OR(Operator):
    def evaluate(self, child_hits: List[List[str]], child_queries: List[ASTNode]):
        return list(set().union(*child_hits))


class NOT(Operator):
    def evaluate(self, child_hits: List[List[str]], child_queries: List[ASTNode]):
        assert len(child_hits) == 2
        return list(set().difference(*child_hits))


class EvalExperiment(RetrievalExperiment):
    operators: Dict[str, Operator] = {
        "AND": AND(),
        "OR": OR(),
        "NOT": NOT()
    }

    @property
    def queries(self) -> Dict[str, ASTNode]:
        if self.ignore_dates:
            return dict([(topic.identifier, self._parsed_queries[i])
                         for i, topic in enumerate(self.collection.topics)])
        # Right at the last step, we can apply the date restrictions.
        return dict([(topic.identifier,
                      OperatorNode("OR",
                                   [self._parsed_queries[i], AtomNode(f"{topic.date_from}:{topic.date_to}", [self.date_field])]
                                   )) for i, topic in enumerate(self.collection.topics)])

    def _parse_queries_process(self, t: Topic):
        if len(t.raw_query) > 0:
            return t, self.query_parser.parse_ast(t.raw_query)
        return None, None

    def _hits_recurse(self, node) -> List[str]:
        if isinstance(node, AtomNode):
            print(node)
            lucene_query = self.query_parser.transform(node)
            hits = self.index.search(lucene_query)
            return [x["id"] for x in hits]
        elif isinstance(node, OperatorNode):
            child_hits = []
            print(node.operator)
            for child in node.children:
                child_hits.append(self._hits_recurse(child))
            self.operators[node.operator.upper()].evaluate(child_hits, node.children)

    def _retrieval(self) -> List[ScoredDoc]:
        for query_id, ast_node in tqdm(self.queries.items(), desc="retrieval"):
            hits = self._hits_recurse(ast_node)
            for hit in hits:
                yield ScoredDoc(query_id, hit, 0)
            break
        self.date_completed = datetime.now()
