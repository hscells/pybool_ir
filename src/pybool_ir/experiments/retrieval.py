"""
Classes and methods for running retrieval experiments.
"""

import hashlib
import json
import uuid
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import ir_measures
from ir_measures import Measure, Recall, Precision, SetF, ScoredDoc
from lupyne import engine
from tqdm import tqdm

import pybool_ir
from pybool_ir.experiments.collections import Collection, Topic
from pybool_ir.index.generic import GenericSearcher
from pybool_ir.index.index import Indexer
from pybool_ir.query.ast import AtomNode
from pybool_ir.query.parser import QueryParser
from pybool_ir.query.pubmed.parser import PubmedQueryParser, Q


class LuceneSearcher(ABC):
    """
    Basic wrapper around a lucene index that provides a simple interface for searching.
    This class can be used as a context manager, which will automatically open and close the index.
    It is possible to directly use this class to do experiments, but the other classes in this module provide a more convenient interface.
    """

    def __init__(self, indexer: Indexer):
        #: The pybool_ir `pybool_ir.index.index.Indexer` class that is used to open the index.
        self.indexer = indexer
        #: The underlying lucene index.
        self.index: engine.Indexer

    # The following two methods provide the `with [..] as [..]` syntax.
    def __enter__(self):
        self.indexer.__enter__()
        self.index = self.indexer.index
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.index = None
        self.indexer.__exit__(exc_type, exc_val, exc_tb)


class GenericLuceneSearcher(LuceneSearcher):
    """
    Wrapper around a lucene index, where the type of the indexer is not known.
    """

    def __init__(self, index_path: Path | str):
        indexer = GenericSearcher(index_path)
        super().__init__(indexer)


class RetrievalExperiment(LuceneSearcher):
    """
    This class provides a convenient interface for running retrieval experiments.
    It can be used as a context manager, which will automatically open and close the index.
    This class should be used for simple experiments where a collection of queries is executed on the index, for example:

    >>> from pybool_ir.experiments.collections import load_collection
    >>> from pybool_ir.experiments.retrieval import RetrievalExperiment
    >>> from pybool_ir.index.pubmed import PubmedIndexer
    >>> from ir_measures import *
    >>> import ir_measures
    >>>
    >>> # Automatically downloads, then loads this collection.
    >>> collection = load_collection("ielab/sysrev-seed-collection")
    >>> # Point the experiment to your index, your collection.
    >>> with RetrievalExperiment(PubmedIndexer(index_path="pubmed"),
    ...                                        collection=collection) as experiment:
    ...     # Get the run of the experiment.
    ...     # This automatically executes the queries.
    ...     run = experiment.run
    >>> # Evaluate the run using ir_measures.
    >>> ir_measures.calc_aggregate([SetP, SetR, SetF], collection.qrels, run)
    """

    def __init__(self, indexer: Indexer, collection: Collection,
                 query_parser: QueryParser = PubmedQueryParser(),
                 eval_measures: List[Measure] = None,
                 run_path: Path = None, filter_topics: List[str] = None,
                 ignore_dates: bool = False, date_field: str = "dp"):
        super().__init__(indexer)
        self.ignore_dates = ignore_dates
        self.date_field = date_field
        # Some arguments have default values that need updating.
        if eval_measures is None:
            eval_measures = [Precision, Recall, SetF]
        if filter_topics is not None:
            filtered_topics = list(filter(lambda x: x.identifier in filter_topics, collection.topics))
            filtered_qrels = list(filter(lambda x: x.query_id in filter_topics, collection.qrels))
            collection = Collection(collection.identifier, filtered_topics, filtered_qrels)

        # Timings for reproducibility and sanity checks.
        self.date_created = datetime.now()
        self.date_completed = None

        # Some internal variables.
        # The run is cached from the retrieval, so that the retrieval is only executed once.
        self._run = None
        # The identifier can uniquely refer to an experiment.
        self._identifier = str(uuid.uuid4())

        # Variables required to run experiments.
        self.run_path = run_path
        self.collection = collection
        self.eval_measures = eval_measures
        self.query_parser = query_parser

        self._parsed_queries = []

        filtered_topics = []
        filtered_qrels = []

        # parsed_queries = process_map(self._parse_queries_process, self.collection.topics, desc="query parsing", max_workers=1)
        parsed_queries = []
        for topic in tqdm(self.collection.topics, desc="parsing queries"):
            parsed_queries.append(self._parse_queries_process(topic))

        for topic, parsed_query in parsed_queries:
            if topic is None:
                continue
            self._parsed_queries.append(parsed_query)
            filtered_topics.append(topic)
            filtered_qrels += [x for x in collection.qrels if x.query_id == topic.identifier]
        self.collection = Collection(collection.identifier, filtered_topics, filtered_qrels)
        self.load()

    def load(self):
        pass

    def _parse_queries_process(self, t: Topic):
        if len(t.raw_query) > 0:
            return t, self.query_parser.parse_lucene(t.raw_query)
        return None, None

    @property
    def queries(self) -> Dict[str, Q]:
        if self.ignore_dates:
            return dict([(topic.identifier, self._parsed_queries[i])
                         for i, topic in enumerate(self.collection.topics)])
        # Right at the last step, we can apply the date restrictions.
        return dict([(topic.identifier,
                      Q.all(
                          *[self._parsed_queries[i]] +
                           [self.query_parser.transform(AtomNode(f"{topic.date_from}:{topic.date_to}", self.date_field))]
                      )) for i, topic in enumerate(self.collection.topics)])

    def count(self) -> List[int]:
        for query_id, lucene_query in tqdm(self.queries.items(), desc="count"):
            yield self.index.count(lucene_query)

    # This private method runs the retrieval.
    def _retrieval(self) -> List[ScoredDoc]:
        for query_id, lucene_query in tqdm(self.queries.items(), desc="retrieval"):
            # Documents can remain un-scored for efficiency (?).
            hits = self.index.search(lucene_query, scored=False)
            for hit in hits:
                yield ScoredDoc(query_id, hit["id"], 0)
        self.date_completed = datetime.now()

    def doc(self, pmid: str):
        hits = self.index.search(f"id:{pmid}")
        for hit in hits:
            # article: ix.PubmedArticle = ix.PubmedArticle.from_dict(hit.dict("mesh_heading_list",
            #                                                                 "mesh_qualifier_list",
            #                                                                 "mesh_major_heading_list",
            #                                                                 "keyword_list",
            #                                                                 "publication_type",
            #                                                                 "supplementary_concept_list"))
            return hit
        return None

    # This is what one would actually call to get the runs.
    @property
    def run(self) -> List[ScoredDoc]:
        # If the experiment has already been executed, just return the results.
        if self._run is not None:
            return self._run

        # If the experimenter doesn't want to write a run file, just return the results.
        if self.run_path is None:
            self._run = list(self._retrieval())
            return self._run

        # Otherwise, we can just iteratively write results as they come to the run file.
        scored_docs = []
        with open(self.run_path, "w") as f:
            # This trick using setdefault below comes from the ir_measures library.
            ranks = {}
            for scored_doc in self._retrieval():
                # Pretty neat way to keep track of topics!
                key = scored_doc.query_id
                rank = ranks.setdefault(key, 0)

                # Write the results and append to our temporary list.
                f.write(f"{scored_doc.query_id} Q0 {scored_doc.doc_id} {rank} {1 + scored_doc.score} {self._identifier[:7]}\n")
                scored_docs.append(scored_doc)

        self._run = scored_docs
        return self._run

    def go(self) -> None:
        """
        Run the experiment without returning anything.
        """
        self._retrieval()

    # Helper methods for evaluating the run.
    # Note that the evaluation methods are part of the "experiment".
    def results(self, aggregate: bool = True):
        if aggregate:
            return ir_measures.calc_aggregate(self.eval_measures, self.collection.qrels, self.run)
        return ir_measures.iter_calc(self.eval_measures, self.collection.qrels, self.run)

    def __hash__(self):
        return hash(pybool_ir.__version__ +
                    str(self.indexer.index_path.absolute()) +
                    str(hash(self.collection)) +
                    str("".join(str(e) for e in self.eval_measures)))

    def __repr__(self):
        d = {
            "pybool_ir.version": pybool_ir.__version__,
            "experiment.identifier": self._identifier,
            "experiment.hash": hashlib.sha256(bytes(str(hash(self)), encoding="utf-8")).hexdigest(),
            "experiment.creation": str(self.date_created),
            "experiment.completed": str(self.date_completed),
            "experiment.collection.identifier": self.collection.identifier,
            "experiment.collection.topics": len(self.collection.topics),
            "experiment.collection.hash": hashlib.sha256(bytes(str(hash(self.collection)), encoding="utf-8")).hexdigest(),
            "index.indexer": str(type(self.indexer)),
            "index.path": str(self.indexer.index_path.absolute()),
        }
        return json.dumps(d)


def AdHocExperiment(indexer: Indexer, raw_query: str = None,
                    query_parser: QueryParser = PubmedQueryParser(),
                    date_from="1900/01/01", date_to="3000/01/01", ignore_dates: bool = False, date_field: str = "dp") -> RetrievalExperiment:
    """
    Unlike the `RetrievalExperiment` class, which expects a `Collection` object, this class allows for ad-hoc queries to be run, for example:

    >>> from pybool_ir.experiments.retrieval import AdHocExperiment
    >>> from pybool_ir.index.pubmed import PubmedIndexer
    >>>
    >>> with AdHocExperiment(PubmedIndexer(index_path="pubmed"), raw_query="headache[tiab]") as experiment:
    >>>     print(experiment.count())
    """
    collection = Collection("adhoc", [], [])
    if raw_query is not None:
        collection = Collection("adhoc", [Topic(identifier="0",
                                                description="ad-hoc topic",
                                                raw_query=raw_query,
                                                date_from=date_from,
                                                date_to=date_to)], [])
    return RetrievalExperiment(indexer, collection, query_parser, ignore_dates=ignore_dates, date_field=date_field)
