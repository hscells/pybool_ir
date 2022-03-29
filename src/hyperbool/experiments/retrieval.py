import hashlib
import uuid
from datetime import datetime
from pathlib import Path
from typing import List

import ir_measures
from ir_measures import Measure, Recall, Precision, SetF, ScoredDoc
from lupyne import engine

import hyperbool
import hyperbool.pubmed.index as ix
from hyperbool.experiments.collections import Collection
from hyperbool.query.parser import PubmedQueryParser


class RetrievalExperiment:
    def __init__(self, index_path: Path, collection: Collection,
                 query_parser: PubmedQueryParser = PubmedQueryParser(),
                 eval_measures: List[Measure] = None, run_path: Path = None):
        if eval_measures is None:
            eval_measures = [Precision, Recall, SetF]

        self.date_created = datetime.now()
        self.date_completed = None

        self._run = None
        self._evaluation = None
        self._identifier = str(uuid.uuid4())

        self.run_path = run_path
        self.index_path = index_path
        self.index: engine.Indexer = None
        self.collection = collection
        self.eval_measures = eval_measures
        self.queries = dict([(topic.identifier, query_parser.parse(topic.raw_query)) for topic in collection.topics])

    def __retrieval(self) -> List[ScoredDoc]:
        for query_id, lucene_query in self.queries.items():
            hits = self.index.search(lucene_query, scored=False)
            for hit in hits:
                yield ScoredDoc(query_id, hit["pmid"], 0)
        self.date_completed = datetime.now()

    def __enter__(self):
        self.index = ix.load_index(self.index_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.index.close()

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
                f.write(f"{scored_doc.query_id} Q0 {scored_doc.doc_id} {rank} {scored_doc.score} {self._identifier[:7]}\n")
                scored_docs.append(scored_doc)

        self._run = scored_doc
        return self._run

    def results(self, aggregate: bool = True):
        if aggregate:
            return ir_measures.calc_aggregate(self.eval_measures, self.collection.qrels, self.run)
        return ir_measures.iter_calc(self.eval_measures, self.collection.qrels, self.run)

    def __hash__(self):
        return hash(hyperbool.__version__ +
                    self.index_path.absolute() +
                    str(hash(self.collection)) +
                    str("".join(str(e) for e in self.eval_measures)))

    def __repr__(self):
        return {
            "hyperbool.version": hyperbool.__version__,
            "experiment.identifier": self._identifier,
            "experiment.hash": hashlib.sha256(hash(self)),
            "experiment.creation": self.date_created,
            "experiment.completed": self.date_completed,
            "experiment.collection.identifier": self.collection.identifier,
            "experiment.collection.topics": len(self.collection.topics),
            "experiment.collection.hash": hashlib.sha256(hash(self.collection)),
            "index.path": self.index_path.absolute(),
            "index.count": self.index.indexSearcher.count(),
        }
