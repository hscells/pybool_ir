from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Callable

import lucene
from lupyne import engine
from ir_measures import ScoredDoc
from tqdm.auto import tqdm

from pybool_ir.experiments.collections import Topic
from pybool_ir.experiments.retrieval import RetrievalExperiment
from pybool_ir.query.ast import ASTNode, OperatorNode, AtomNode
from pybool_ir.query.pubmed.parser import FieldUnit

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query


class Operator(ABC):
    """
     An operator combines document sets together.
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

class WeakOperator(Operator):

    def __init__(self, theta: float, alpha: int = 1, k: int = 2, compare_func: Callable[[float, float], bool] = lambda a, b: a > b):
        super(WeakOperator, self).__init__()
        self.theta = theta
        # ==========================================================
        self.OR = OR()  # Used to create a unique set of all documents.
        self.alpha = alpha
        self.k = k

        self.compare_func = compare_func

    def inclusion_probability(self, target_doc: int, other_lists: List[List[int]]):
        # prob_d = sum([1 if target_doc in other_list else 0 for other_list in other_lists]) / len(other_lists)
        prob_cd = 1
        prob_d = 0
        for other_list in other_lists:
            try:
                pos = other_list.index(target_doc)
                prob_d += 1
            except ValueError:
                pos = len(other_list)
            prob_cd *= (self.alpha + pos) / (len(other_list) + (self.alpha * self.k))
        prob_d = prob_d / len(other_lists)
        return (prob_d * prob_cd) / ((prob_d * prob_cd) + ((1 - prob_d) * (1 - prob_cd)))

    def evaluate(self, child_hits: List[List[int]], child_queries: List[ASTNode]) -> List[int]:
        docs = self.OR.evaluate(child_hits, child_queries)
        new_docs = [doc for doc in docs if self.compare_func(self.inclusion_probability(doc, child_hits), self.theta)]
        print(len(docs), len(new_docs))
        return new_docs


class WeakAND(WeakOperator):
    def __init__(self, theta: float = 0.9):
        super(WeakAND, self).__init__(theta=theta)


class WeakOR(WeakOperator):
    def __init__(self, theta: float = 0.0):
        super(WeakOR, self).__init__(theta=theta)


class WeakNOT(WeakOperator):
    def __init__(self, theta: float = 0.9, compare_func=lambda a, b: a < b):
        super(WeakNOT, self).__init__(theta=theta, compare_func=compare_func)


# --------------------------------------


class EvalExperiment(RetrievalExperiment):

    def load(self):
        self.operators: Dict[str, Operator] = {
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
                      OperatorNode("AND",
                                   [self._parsed_queries[i], AtomNode(f"{topic.date_from}:{topic.date_to}", self.date_field)]
                                   )) for i, topic in enumerate(self.collection.topics)])

    def _parse_queries_process(self, t: Topic):
        if len(t.raw_query) > 0:
            return t, self.query_parser.parse_ast(t.raw_query)
        return None, None

    def query_atom(self, node: AtomNode) -> List[int]:
        lucene_query = self.query_parser.transform(node)
        hits = self.index.search(lucene_query, scores=True)
        # print(node, len(hits))
        return hits.ids

    def query_operator(self, node: OperatorNode) -> List[int]:
        child_hits = []
        for child in node.children:
            child_hits.append(list(self.hits_recurse(child)))
        hits = self.operators[node.operator.upper()].evaluate(child_hits, node.children)
        # print(node.operator, len(hits))
        return hits

    def hits_recurse(self, node) -> List[int]:
        if isinstance(node, AtomNode):
            # Borked parsing issue.
            if isinstance(node.field, FieldUnit):
                node.field = node.field.field
            return self.query_atom(node)
        elif isinstance(node, OperatorNode):
            return self.query_operator(node)

    def _retrieval(self) -> List[ScoredDoc]:
        for query_id, ast_node in tqdm(self.queries.items(), desc="retrieval"):
            lucene_ids = self.hits_recurse(ast_node)
            for lucene_id in lucene_ids:
                doc = self.index.get(lucene_id, "id")
                yield ScoredDoc(query_id, doc["id"], 0)
        self.date_completed = datetime.now()


# --------------------------------------


class BoostedEvalExperiment(EvalExperiment):
    def query_atom(self, node: AtomNode, boosting_terms: List[str] = None) -> List[int]:
        lucene_query = self.query_parser.transform(node)
        query = Q.boost(lucene_query, 3.0) | Q.any(*[Q.any(*[Q.boost(Q.span("title", x), 1.5) for x in boosting_terms]), Q.any(*[Q.boost(Q.span("abstract", x), 1.0) for x in boosting_terms])])
        hits = self.index.search(query, scores=True, count=1_000)
        # print(node, len(hits))
        return hits.ids

    def query_operator(self, node: OperatorNode) -> List[int]:
        do_lucene_or = node.operator == "OR"

        boosting_terms = []
        for child in node.children:
            if isinstance(child, AtomNode):
                boosting_terms.append(child.query)
            if isinstance(child, OperatorNode):
                do_lucene_or = False

        if do_lucene_or:
            return self.index.search(self.query_parser.transform(node), scores=True, count=10_000).ids

        child_hits = []
        for child in node.children:
            child_hits.append(list(self.hits_recurse(child, boosting_terms)))
        hits = self.operators[node.operator.upper()].evaluate(child_hits, node.children)
        # print(node.operator, len(hits))
        return hits

    def hits_recurse(self, node, boosting_terms: List[str] = None) -> List[int]:
        if boosting_terms is None:
            boosting_terms = []
        if isinstance(node, AtomNode):
            # Borked parsing issue.
            if isinstance(node.field, FieldUnit):
                node.field = node.field.field
            return self.query_atom(node, boosting_terms)
        elif isinstance(node, OperatorNode):
            return self.query_operator(node)
