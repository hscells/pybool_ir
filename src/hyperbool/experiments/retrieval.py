import hashlib
import json
import multiprocessing
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import ir_measures
from ir_measures import Measure, Recall, Precision, SetF, ScoredDoc, Qrel
from lupyne import engine
from tqdm.contrib.concurrent import process_map
from tqdm.auto import tqdm

import hyperbool
import hyperbool.pubmed.index as ix
from hyperbool.experiments.collections import Collection, Topic
from hyperbool.query.parser import PubmedQueryParser, Q

class LuceneSearcher:
    def __init__(self, index_path: Path):
        if isinstance(index_path, str):
            index_path = Path(index_path)
        self.index_path = index_path
        self.index = None

    # The following two methods provide the `with [..] as [..]` syntax.
    def __enter__(self):
        self.index = ix.load_index(self.index_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.index.close()
        self.index = None


class RetrievalExperiment(LuceneSearcher):
    def __init__(self, index_path: Path, collection: Collection,
                 query_parser: PubmedQueryParser = PubmedQueryParser(),
                 eval_measures: List[Measure] = None,
                 run_path: Path = None, filter_topics: List[str] = None):
        super().__init__(index_path)
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

        parsed_queries = process_map(self._parse_queries_process, self.collection.topics, desc="query parsing")
        for topic, parsed_query in parsed_queries:
            self._parsed_queries.append(parsed_query)
            filtered_topics.append(topic)
            filtered_qrels += [x for x in collection.qrels if x.query_id == topic.identifier]

        self.collection = Collection(collection.identifier, filtered_topics, filtered_qrels)

    def _parse_queries_process(self, t: Topic):
        if len(t.raw_query) > 0:
            return t, self.query_parser.parse(t.raw_query)
        return None, None

    @property
    def queries(self):
        # Right at the last step, we can apply the date restrictions.
        return dict([(topic.identifier,
                      Q.all(
                          *[self.query_parser.node_to_lucene(self._parsed_queries[i])] +
                           [self.query_parser.parse_lucene(f"{topic.date_from}:{topic.date_to}[dp]")]
                      )) for i, topic in enumerate(self.collection.topics)])

    def count(self) -> List[int]:
        for query_id, lucene_query in tqdm(self.queries.items(), desc="count"):
            yield self.index.count(lucene_query)

    # This private method runs the retrieval.
    def __retrieval(self) -> List[ScoredDoc]:
        for query_id, lucene_query in tqdm(self.queries.items(), desc="retrieval"):
            # Documents can remain un-scored for efficiency (?).
            hits = self.index.search(lucene_query, scored=False)
            for hit in hits:
                yield ScoredDoc(query_id, hit["pmid"], 0)
        self.date_completed = datetime.now()

    def doc(self, pmid: str):
        hits = self.index.search(f"pmid:{pmid}")
        for hit in hits:
            article: ix.PubmedArticle = ix.PubmedArticle.from_dict(hit.dict("mesh_heading_list",
                                                                            "mesh_qualifier_list",
                                                                            "mesh_major_heading_list",
                                                                            "keyword_list",
                                                                            "publication_type",
                                                                            "supplementary_concept_list"))
            return article
        return None

    # This is what one would actually call to get the runs.
    @property
    def run(self) -> List[ScoredDoc]:
        # If the experiment has already been executed, just return the results.
        if self._run is not None:
            return self._run

        # If the experimenter doesn't want to write a run file, just return the results.
        if self.run_path is None:
            self._run = list(self.__retrieval())
            return self._run

        # Otherwise, we can just iteratively write results as they come to the run file.
        scored_docs = []
        with open(self.run_path, "w") as f:
            # This trick using setdefault below comes from the ir_measures library.
            ranks = {}
            for scored_doc in self.__retrieval():
                # Pretty neat way to keep track of topics!
                key = scored_doc.query_id
                rank = ranks.setdefault(key, 0)

                # Write the results and append to our temporary list.
                f.write(f"{scored_doc.query_id} Q0 {scored_doc.doc_id} {rank} {1 + scored_doc.score} {self._identifier[:7]}\n")
                scored_docs.append(scored_doc)

        self._run = scored_docs
        return self._run

    # Helper methods for evaluating the run.
    # Note that the evaluation methods are part of the "experiment".
    def results(self, aggregate: bool = True):
        if aggregate:
            return ir_measures.calc_aggregate(self.eval_measures, self.collection.qrels, self.run)
        return ir_measures.iter_calc(self.eval_measures, self.collection.qrels, self.run)

    def __hash__(self):
        return hash(hyperbool.__version__ +
                    str(self.index_path.absolute()) +
                    str(hash(self.collection)) +
                    str("".join(str(e) for e in self.eval_measures)))

    def __repr__(self):
        d = {
            "hyperbool.version": hyperbool.__version__,
            "experiment.identifier": self._identifier,
            "experiment.hash": hashlib.sha256(bytes(str(hash(self)), encoding="utf-8")).hexdigest(),
            "experiment.creation": str(self.date_created),
            "experiment.completed": str(self.date_completed),
            "experiment.collection.identifier": self.collection.identifier,
            "experiment.collection.topics": len(self.collection.topics),
            "experiment.collection.hash": hashlib.sha256(bytes(str(hash(self.collection)), encoding="utf-8")).hexdigest(),
            "index.path": str(self.index_path.absolute()),
        }
        return json.dumps(d)


def AdHocExperiment(index_path: Path, raw_query: str,
                    query_parser: PubmedQueryParser = PubmedQueryParser(),
                    date_from="1900/01/01", date_to="3000/01/01") -> RetrievalExperiment:
    collection = Collection("adhoc", [Topic(identifier="0",
                                            description="ad-hoc topic",
                                            raw_query=raw_query,
                                            date_from=date_from,
                                            date_to=date_to)], [])
    return RetrievalExperiment(index_path, collection, query_parser)
