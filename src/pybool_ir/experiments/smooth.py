"""
Classes and methods for the paper "Smooth Operators for Effective Systematic Review Queries".

TODO: This is quite messy. Needs to be cleaned up.
NB: See the bottom of this file for experiments and more details, like accessing the data.
"""
import os
import pickle
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

import appdirs
import ir_measures
import lucene
import numpy as np
import pandas as pd
from fnvhash import fnv1a_32
from ir_measures import ScoredDoc, Qrel
from lupyne import engine
from sklearn.tree import DecisionTreeRegressor, BaseDecisionTree
from tqdm import tqdm

from pybool_ir.experiments import decompose
from pybool_ir.experiments.collections import Topic
from pybool_ir.experiments.decompose import Operator
from pybool_ir.experiments.retrieval import RetrievalExperiment
from pybool_ir.query.ast import ASTNode, OperatorNode, AtomNode
from pybool_ir.query.pubmed.parser import _FieldUnit

assert lucene.getVMEnv() or lucene.initVM()
Q = engine.Query


# --------------------------------------

class SmoothOperator(Operator, ABC):

    def __init__(self, theta: float):
        super(SmoothOperator, self).__init__()
        self.theta = theta
        # ==========================================================
        self.OR = decompose.OR()  # Used to create a unique set of all documents.

    @abstractmethod
    def compare_func(self, prob: float, theta: float):
        pass

    def get_docdicts(self, child_hits, child_queries):
        docs = self.OR.evaluate(child_hits, child_queries)
        # print(len(child_hits), len(docs))
        doc_dicts = []
        for child_list in child_hits:
            doc_dicts.append(dict([(d, i + 1) for i, d in enumerate(child_list)]))
        return doc_dicts, docs

    def calc_scores(self, child_hits: List[List[int]], child_queries: List[ASTNode]) -> List[Tuple[int, float, float]]:
        doc_dicts, docs = self.get_docdicts(child_hits, child_queries)
        scored_docs = []
        rrf_k = 10_000
        num_q = len(doc_dicts)
        for doc in docs:
            prob_i, rrf_mnz, rrf_score = self.calc_probi(doc, doc_dicts, num_q, rrf_k)

            scored_docs.append((doc, prob_i, rrf_mnz * rrf_score))

        # Sort documents by RRFMNZ.
        scored_docs.sort(key=lambda x: x[2], reverse=True)

        return scored_docs

    def score_docs(self, child_hits: List[List[int]], child_queries: List[ASTNode]) -> List[Tuple[int, float, float]]:
        doc_dicts, docs = self.get_docdicts(child_hits, child_queries)
        scored_docs = []
        rrf_k = 10_000
        num_q = len(doc_dicts)
        for doc in docs:
            prob_i, rrf_mnz, rrf_score = self.calc_probi(doc, doc_dicts, num_q, rrf_k)

            if self.compare_func(prob_i, self.theta):
                scored_docs.append((doc, prob_i, rrf_mnz * rrf_score))

        # Sort documents by RRFMNZ.
        scored_docs.sort(key=lambda x: x[2], reverse=True)

        return scored_docs

    def calc_probi(self, doc, doc_dicts, num_q, rrf_k):
        prob_cd = 1
        prob_d = 0
        rrf_score = 0
        for other_list in doc_dicts:
            if doc in other_list:
                pos = other_list[doc]
                prob_d += 1
                prob_cd *= (1 - ((1 + pos) / (2 + (len(other_list)))))
                rrf_score += 1 / (rrf_k + pos)
        rrf_mnz = prob_d
        prob_d = prob_d / num_q
        if prob_d == 1 and prob_cd == 0:
            print(prob_d, prob_cd, (prob_d * prob_cd), ((prob_d * prob_cd) + ((1 - prob_d) * (1 - prob_cd))))
            return 1, rrf_mnz, rrf_score

        prob_i = (prob_d * prob_cd) / ((prob_d * prob_cd) + ((1 - prob_d) * (1 - prob_cd)))
        return prob_i, rrf_mnz, rrf_score

    def evaluate(self, child_hits: List[List[int]], child_queries: List[ASTNode]) -> List[int]:
        scored_docs = self.score_docs(child_hits, child_queries)
        # Only return document ids.
        new_docs = [doc[0] for doc in scored_docs]
        return new_docs


class SmoothAND(SmoothOperator):
    def __init__(self, theta: float = 1.0):
        super(SmoothAND, self).__init__(theta=theta)

    def compare_func(self, prob: float, theta: float):
        return prob >= theta


class SmoothOR(SmoothOperator):
    def __init__(self, theta: float = 0.0):
        super(SmoothOR, self).__init__(theta=theta)

    def compare_func(self, prob: float, theta: float):
        return prob >= theta


class SmoothNOT(SmoothOperator):
    def __init__(self, theta: float = 1.0):
        super(SmoothNOT, self).__init__(theta=theta)

    def compare_func(self, prob: float, theta: float):
        return prob < theta


class SmoothOperatorPredictorMixin(SmoothOperator, ABC):
    def __init__(self, clf: BaseDecisionTree, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clf = clf

    def predict(self, depth: float, children: float, numret: float, child_avg_numret, child_std_numret, num_child_ops, num_child_atm) -> float:
        df = pd.DataFrame([{"depth": depth, "children": children, "numret": numret,
                            "child_avg_numret": child_avg_numret, "child_std_numret": child_std_numret,
                            "num_child_ops": num_child_ops, "num_child_atm": num_child_atm}])
        return float(self.clf.predict(df)[0])


class SmoothANDPredictor(SmoothOperatorPredictorMixin, SmoothAND):
    def __init__(self, clf: DecisionTreeRegressor, theta: float = 1.0):
        super(SmoothANDPredictor, self).__init__(clf, theta)


class SmoothORPredictor(SmoothOperatorPredictorMixin, SmoothOR):
    def __init__(self, clf: DecisionTreeRegressor, theta: float = 0.0):
        super(SmoothORPredictor, self).__init__(clf, theta)


class SmoothNOTPredictor(SmoothOperatorPredictorMixin, SmoothOR):
    def __init__(self, clf: DecisionTreeRegressor, theta: float = 1.0):
        super(SmoothNOTPredictor, self).__init__(clf, theta)


# --------------------------------------


class QueryExperiment(RetrievalExperiment):
    """
    Base class for running experiments that involve execution of each atomic node in a query.
    """

    def load(self):
        self.operators: Dict[str, SmoothOperator] = {
            "AND": SmoothAND(),
            "OR": SmoothOR(),
            "NOT": SmoothNOT()
        }
        self.cache = True
        self.cache_dir = Path(appdirs.user_data_dir("pybool_ir")) / "eval_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        if self.cache:
            print(f"cache: {self.cache_dir}")
        self.topics = {}
        for t in self.collection.topics:
            self.topics[t.identifier] = t

    @property
    def queries(self) -> Dict[str, ASTNode]:
        # FYI: Date restrictions are applied only to atomic clauses.
        return dict([(topic.identifier, self._parsed_queries[i])
                     for i, topic in enumerate(self.collection.topics)])

    def _parse_queries_process(self, t: Topic):
        if len(t.raw_query) > 0:
            return t, self.query_parser.parse_ast(t.raw_query)
        return None, None

    def query_atom(self, node: AtomNode, query_id) -> List[int]:
        # Apply dates here.
        if not self.ignore_dates:
            topic = self.topics[query_id]
            node = OperatorNode("AND", [node, AtomNode(f"{topic.date_from}:{topic.date_to}", self.date_field)])
        lucene_query = self.query_parser.transform(node)
        count = None

        # http://isthe.com/chongo/tech/comp/fnv/#FNV-1a
        cache_id = str(fnv1a_32(bytes(f"{node}{count}", encoding="utf-8")))
        if cache_id in os.listdir(self.cache_dir):
            print("[*]", node)
            with open(self.cache_dir / cache_id, "rb") as f:
                return pickle.load(f)
        print("[ ]", node)
        hits = list(self.index.search(lucene_query, count=count).ids)
        with open(self.cache_dir / cache_id, "wb") as f:
            pickle.dump(hits, f)
        return hits

    def query_operator(self, node: OperatorNode, query_id) -> List[int]:
        child_hits = []
        for i, child in enumerate(node.children):
            child_hits.append(list(self.hits_recurse(child, query_id)))

        print(node.operator, [len(x) for x in child_hits])

        if node.operator == "NOT":
            # First, apply the NOT operation.
            results = self.operators[node.operator.upper()].evaluate(child_hits, node.children)
            # Then, apply the AND operation between the first child and the result.
            results = SmoothAND(theta=1.0).evaluate([child_hits[0], results], [node.children[0], None])
        else:
            results = self.operators[node.operator.upper()].evaluate(child_hits, node.children)

        print(node.operator, [len(x) for x in child_hits], len(results))
        return results

    def hits_recurse(self, node, query_id) -> List[int | str]:
        if isinstance(node, AtomNode):
            # Borked parsing issue.
            if isinstance(node.field, _FieldUnit):
                node.field = node.field.field
            return self.query_atom(node, query_id)
        elif isinstance(node, OperatorNode):
            return self.query_operator(node, query_id)

    def _retrieval(self) -> List[ScoredDoc]:
        for query_id, ast_node in tqdm(self.queries.items(), desc="retrieval"):
            assert isinstance(ast_node, OperatorNode)
            lucene_ids = self.hits_recurse(ast_node, query_id)
            for i, lucene_id in enumerate(lucene_ids):
                doc = self.index.get(lucene_id, "id")
                yield ScoredDoc(query_id, doc["id"], len(lucene_ids) - i)
        self.date_completed = datetime.now()


class PrecomputedQueryExperiment(QueryExperiment):
    """
    Variant of the QueryExperiment that uses precomputed results (i.e., from dense retrieval).
    """

    # Kind of a hack.
    atom_mapping: Dict[str, str]  # cache_id -> dense_id
    dense_folder: str

    def query_atom(self, node: AtomNode, query_id) -> List[str]:
        # Apply dates here.
        if not self.ignore_dates:
            topic = self.topics[query_id]
            node = OperatorNode("AND", [node, AtomNode(f"{topic.date_from}:{topic.date_to}", self.date_field)])

        lucene_query = self.query_parser.transform(node)
        count = None

        # http://isthe.com/chongo/tech/comp/fnv/#FNV-1a
        cache_id = str(fnv1a_32(bytes(f"{node}{count}", encoding="utf-8")))
        dense_id = None
        if cache_id in self.atom_mapping:
            dense_id = self.atom_mapping[cache_id]
        dense_ranking = f"{self.dense_folder}/{dense_id}.tsv"
        if (dense_id is None) or (not os.path.exists(dense_ranking)):
            print("dense ranking not found:", dense_ranking)
            hits = list(self.index.search(lucene_query, count=count).ids)
            pmids = []
            for hit in hits:
                doc = self.index.get(hit, "id")
                pmids.append(doc["id"])
        else:
            with open(dense_ranking, "r") as f:
                pmids = [line.strip() for line in f.readlines()]

        print(f"{node}, {len(pmids)}")
        return pmids

    def _retrieval(self) -> List[ScoredDoc]:
        for query_id, ast_node in tqdm(self.queries.items(), desc="retrieval"):
            assert isinstance(ast_node, OperatorNode)
            pmids = self.hits_recurse(ast_node, query_id)
            for i, pmid in enumerate(pmids):
                yield ScoredDoc(query_id, pmid, len(pmids) - i)
        self.date_completed = datetime.now()


class PredictorQueryExperiment(QueryExperiment):
    """
    Variant of the QueryExperiment that uses a pre-trained predictor for estimating the theta parameter.
    """

    def query_operator(self, node: OperatorNode, query_id, depth=0) -> (List[int], ASTNode):
        node_copy = OperatorNode(operator=node.operator, children=[])

        # Recursively get the hits for each child.
        child_hits = []
        for i, child in enumerate(node.children):
            hits, child_node_copy = self.hits_recurse(child, query_id, depth + 1)
            if isinstance(child, OperatorNode):
                node_copy.children.append(child_node_copy)
            else:
                node_copy.children.append(child)
            child_hits.append(list(hits))

        # Calculate theta for this operator.
        op = self.operators[node.operator.upper()]
        if isinstance(op, SmoothOperatorPredictorMixin):
            old_theta = self.operators[node.operator.upper()].theta
            depth = depth
            children = len(node.children)
            self.query_parser.format(node)
            numret = self.index.indexSearcher.count(self.query_parser.transform(node))
            child_ret = []

            num_child_ops = 0
            num_child_atm = 0
            for child in node.children:
                child_ret.append(self.index.indexSearcher.count(self.query_parser.transform(child)))
                if isinstance(child, AtomNode):
                    num_child_atm += 1
                if isinstance(child, OperatorNode):
                    num_child_ops += 1
            avg_child_ret = np.mean(child_ret)
            std_child_ret = np.std(child_ret)

            theta = op.predict(depth, children, numret,
                               avg_child_ret, std_child_ret, num_child_ops, num_child_atm)
        elif isinstance(op, SmoothOperator):
            theta = op.theta
            old_theta = theta
        else:
            raise Exception("PredictorQueryExperiment is only compatible with weak operators")

        # Set the predicted theta value and evaluate the operator.
        self.operators[node.operator.upper()].theta = theta
        results = self.operators[node.operator.upper()].evaluate(child_hits, node.children)
        if node.operator == "NOT":
            # Apply the AND operation between the first child and the result.
            results = SmoothAND(theta=1.0).evaluate([child_hits[0], results], [node.children[0], None])

        # Reset the theta value.
        self.operators[node.operator.upper()].theta = old_theta

        node_op = f"{node.operator}@{theta}"
        print(node_op, [len(x) for x in child_hits], len(results), "-", depth)
        node_copy.operator = node_op
        return results, node_copy

    def hits_recurse(self, node, query_id, depth=0) -> (List[int], ASTNode):
        if isinstance(node, AtomNode):
            # Borked parsing issue.
            if isinstance(node.field, _FieldUnit):
                node.field = node.field.field
            return self.query_atom(node, query_id), None
        elif isinstance(node, OperatorNode):
            return self.query_operator(node, query_id, depth=depth)

    def _retrieval(self) -> List[ScoredDoc]:
        for query_id, ast_node in tqdm(self.queries.items(), desc="retrieval"):
            assert isinstance(ast_node, OperatorNode)
            dir = f"predicted-queries/{self.collection.identifier}"
            os.makedirs(dir, exist_ok=True)
            lucene_ids, node = self.hits_recurse(ast_node, query_id)
            print("writing query...")
            with open(f"{dir}/{query_id}", "w") as f:
                f.write(node.__repr__())
            for i, lucene_id in enumerate(lucene_ids):
                doc = self.index.get(lucene_id, "id")
                yield ScoredDoc(query_id, doc["id"], len(lucene_ids) - i)
        self.date_completed = datetime.now()


class OracleQueryExperiment(QueryExperiment):
    """
    Variant of the QueryExperiment that performs an oracle search to determine an approximately optimal theta value.
    """

    def hits_recurse(self, node, query_id) -> tuple[list[int], ASTNode]:
        if isinstance(node, AtomNode):
            # Borked parsing issue.
            if isinstance(node.field, _FieldUnit):
                node.field = node.field.field
            return self.query_atom(node, query_id), node
        elif isinstance(node, OperatorNode):
            return self.query_operator(node, query_id)

    def query_operator(self, node: OperatorNode, query_id) -> tuple[list[int], ASTNode]:
        print("mapping qrel docids...", end="")
        qrels = [x for x in self.collection.qrels if x.query_id == query_id]
        for i, qrel in enumerate(qrels):
            lucene_ids = list(self.index.search(f"id:{qrel.doc_id}").ids)
            if len(lucene_ids) > 0:
                lucene_id = lucene_ids[0]
            else:
                continue
            # print(qrel.doc_id, lucene_id)
            qrels[i] = Qrel(query_id=qrel.query_id, doc_id=str(lucene_id), relevance=qrel.relevance, iteration=qrel.iteration)
        print("done!")

        if node.operator == "AND":
            op = SmoothAND()
            default_param = 1.0
            theta_params = [1.0, 0.999, 0.99, 0.95, 0.9, 0.8]
        elif node.operator == "OR":
            op = SmoothOR()
            default_param = 0.0
            theta_params = [0.0, 0.001, 0.01, 0.1, 0.15, 0.2]
        elif node.operator == "NOT":
            op = SmoothNOT()
            default_param = 1.0
            theta_params = [1.0, 0.999, 0.99, 0.95, 0.9, 0.8]
        else:
            raise Exception("unknown operator", node.operator)

        child_hits = []
        for i, child in enumerate(node.children):
            hits, _ = self.hits_recurse(child, query_id)
            child_hits.append(list(hits))

        node_op = node.operator

        best_theta = -1.0
        best_r = 0.0
        best_p = 0.0
        best_f = 0.0
        results = []
        print("scoring...", end="")
        scores = op.calc_scores(child_hits, node.children)
        print(f"scored! ({len(scores)})")

        docs = []

        prev_better = False
        early_stop = False
        for i, theta in enumerate(theta_params):
            op.theta = theta

            scored_docs = []
            lucene_ids = []

            for j, score_tup in enumerate(scores):
                if op.compare_func(score_tup[1], op.theta):
                    lucene_ids.append(score_tup[0])
                    scored_docs.append(ScoredDoc(query_id, str(score_tup[0]), len(docs) - j))

            if node.operator == "NOT":
                lucene_ids = SmoothAND(theta=1.0).evaluate([child_hits[0], lucene_ids], [node.children[0], None])

            e = ir_measures.calc_aggregate([ir_measures.SetR, ir_measures.SetP, ir_measures.SetF], qrels, scored_docs)

            if e[ir_measures.SetR] >= best_r and e[ir_measures.SetF] > best_f:
                print(f"[x]:{i}: {theta} r={e[ir_measures.SetR]} p={e[ir_measures.SetP]} f={e[ir_measures.SetF]}")
                best_r = e[ir_measures.SetR]
                best_p = e[ir_measures.SetP]
                best_f = e[ir_measures.SetF]
                best_theta = theta
                results = lucene_ids
                prev_better = True
            else:
                print(f"[ ]:{i}: {theta} r={e[ir_measures.SetR]} p={e[ir_measures.SetP]} f={e[ir_measures.SetF]}")
                if prev_better:
                    early_stop = True

            # We really can stop here; there are no more improvements.
            if early_stop:
                print(f"[!]:~: stopping early")
                break

        if best_theta < 0.0:
            print(f"[!]:~: using default parameter")
            op.theta = default_param
            best_theta = default_param
            results = op.evaluate(child_hits, node.children)

        node_op = f"{node.operator}@{best_theta}"

        print(node_op, [len(x) for x in child_hits], len(results))
        node.operator = node_op
        return results, node

    def _retrieval(self) -> List[ScoredDoc]:
        for query_id, ast_node in tqdm(self.queries.items(), desc="retrieval"):
            assert isinstance(ast_node, OperatorNode)
            dir = f"oracle-queries/{self.collection.identifier}"
            os.makedirs(dir, exist_ok=True)
            lucene_ids, node = self.hits_recurse(ast_node, query_id)
            print("writing query...")
            with open(f"{dir}/{query_id}", "w") as f:
                f.write(node.__repr__())
            for i, lucene_id in enumerate(lucene_ids):
                doc = self.index.get(lucene_id, "id")
                yield ScoredDoc(query_id, doc["id"], len(lucene_ids) - i)
        self.date_completed = datetime.now()


"""
# -------------------------------------------------------------------------------
# The following code is used to create all of the runs for the SIGIR'23 paper
#         Smooth Operators for Effective Systematic Review Queries
# NB: The theta predictors are trained separately, but are available on request.
# NB: The dense models are trained on TRIPCLICK and are therefore unavailable.
# NB: The run files and jupyter notebooks are available on request.
# -------------------------------------------------------------------------------

import json
import joblib

from pybool_ir.experiments.collections import load_collection
from pybool_ir.index.pubmed import PubmedIndexer

# ------------------------- theta predictions -----------------------------------

clf_or = joblib.load("dt_or_srlogs.v4.joblib")
clf_and = joblib.load("dt_and_srlogs.v4.joblib")
# Seems like NOT has too few samples to be useful.
# clf_not = joblib.load("dt_not_srlogs.v2.joblib")

# ------------------- atom mapping (for dense retrieval) -------------------------

# Load the atom mapping
with open("atom-mapping.json", "r") as f:
    atom_mapping = json.load(f)


# ---------------------------- CLEF2018 -----------------------------------------

collection = load_collection("wang/clef-tar/2018/testing")

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2018-equivalents.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2018-and-0.99.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=0.99)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2018-and-0.9.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=0.9)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2018-or-0.1.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.1)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2018-or-0.01.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with OracleQueryExperiment(PubmedIndexer(
        index_path="../indexes/baseline22"), collection=collection,
        run_path=Path(f"clef2018-oracle.v3.run")) as experiment:
    _ = experiment.run
with PredictorQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                              run_path=Path(f"clef2018-predictor.v4.run")) as experiment:
    experiment.operators["AND"] = SmoothANDPredictor(clf=clf_and)
    experiment.operators["OR"] = SmoothORPredictor(clf=clf_or)
    experiment.operators["NOT"] = SmoothNOT()
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2018-dense-pubmedbert-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_pubmed_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2018-dense-bert-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_base_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2018-dense-bert-and+or.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_base_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=0.99)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2018-dense-distil-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_distil_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2018-dense-distil-and+or.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_distil_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=0.99)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

# ---------------------------- CLEF2017 -----------------------------------------

collection = load_collection("wang/clef-tar/2017/testing")
#
with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2017-equivalents.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2017-and-0.99.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=0.99)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2017-and-0.9.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=0.9)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2017-or-0.1.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.1)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"clef2017-or-0.01.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with OracleQueryExperiment(PubmedIndexer(
        index_path="../indexes/baseline22"), collection=collection,
        run_path=Path(f"clef2017-oracle.v3.run")) as experiment:
    _ = experiment.run
with PredictorQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                              run_path=Path(f"clef2017-predictor.v4.run")) as experiment:
    experiment.operators["AND"] = SmoothANDPredictor(clf=clf_and)
    experiment.operators["OR"] = SmoothORPredictor(clf=clf_or)
    experiment.operators["NOT"] = SmoothNOT()
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2017-dense-pubmedbert-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_pubmed_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2017-dense-bert-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_base_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2017-dense-bert-and+or.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_base_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=0.99)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2017-dense-distil-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_distil_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/clef2017-dense-distil-and+or.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_distil_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=0.99)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

# ------------------------------ SEED -------------------------------------------

collection = load_collection("ielab/sysrev-seed-collection")

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"seed-or-0.1.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.1)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with QueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                    run_path=Path(f"seed-or-0.01.run")) as experiment:
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with OracleQueryExperiment(PubmedIndexer(
        index_path="../indexes/baseline22"), collection=collection,
        run_path=Path(f"seed-oracle.v3.run")) as experiment:
    _ = experiment.run


with PredictorQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                              run_path=Path(f"seed-predictor.v4.run")) as experiment:
    experiment.operators["AND"] = SmoothANDPredictor(clf=clf_and)
    experiment.operators["OR"] = SmoothORPredictor(clf=clf_or)
    experiment.operators["NOT"] = SmoothNOT()
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/seed-dense-pubmedbert-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_pubmed_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run
#
with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/seed-dense-bert-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_base_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/seed-dense-bert-and+or.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_base_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=0.99)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/seed-dense-distil-equivalents.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_distil_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=1.0)
    experiment.operators["OR"] = SmoothOR(theta=0.0)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run

with PrecomputedQueryExperiment(PubmedIndexer(index_path="../indexes/baseline22"), collection=collection,
                                run_path=Path(f"sigir2023-experiments/seed-dense-distil-and+or.run")) as experiment:
    experiment.dense_folder = "outfiles-combined_reranked_distil_bert.sorted-pmids-combined"
    experiment.atom_mapping = atom_mapping
    experiment.operators["AND"] = SmoothAND(theta=0.99)
    experiment.operators["OR"] = SmoothOR(theta=0.01)
    experiment.operators["NOT"] = SmoothNOT(theta=1.0)
    _ = experiment.run
"""
