import datetime
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict

import appdirs
from datetime import datetime
import ir_measures
import requests
from dataclasses_json import dataclass_json
from ir_measures import Qrel

from hyperbool import util


@dataclass_json
@dataclass
class Topic:
    identifier: str
    description: str
    raw_query: str
    date_from: str
    date_to: str

    @classmethod
    def from_file(cls, topic_path: Path) -> List["Topic"]:
        with open(topic_path, "r") as f:
            for line in f:
                yield Topic.from_json(line)


@dataclass
class Collection:
    identifier: str
    topics: List[Topic]
    qrels: List[Qrel]

    @classmethod
    def from_dir(cls, collection_path: Path) -> "Collection":
        assert collection_path.is_dir()

        topics_path = collection_path / "topics.jsonl"
        qrels_path = collection_path / "qrels"

        assert qrels_path.is_file()
        with open(qrels_path, "r") as f:
            qrels = list(ir_measures.read_trec_qrels(f))
        topics = list(Topic.from_file(topics_path))

        return Collection(collection_path.name, topics, qrels)

    def __hash__(self):
        return hash("".join([repr(topic) for topic in self.topics] +
                            [repr(qrel) for qrel in self.qrels]))


def load_collection(name: str) -> Collection:
    return __collection_load_methods[name](name)


def __load_sysrev_seed(name: str) -> Collection:
    git_hash = "84d116a1ed2dae191cce64daff7f968323860c53"
    collection_url = f"https://github.com/ielab/sysrev-seed-collection/raw/{git_hash}/collection_data/overall_collection.jsonl"
    download_dir = Path(appdirs.user_data_dir("hyperbool")) / "collections" / name
    raw_collection = download_dir / "raw.jsonl"
    topic_file = download_dir / "topics.jsonl"
    qrels_file = download_dir / "qrels"

    if not download_dir.exists():
        os.makedirs(download_dir, exist_ok=True)
        util.download_file(collection_url, raw_collection)

        if os.path.exists(topic_file):
            os.remove(topic_file)
        if os.path.exists(qrels_file):
            os.remove(qrels_file)

        with open(raw_collection, "r") as cf:
            for line in cf:
                t = json.loads(line)

                with open(topic_file, "a") as f:
                    if t["id"] == "1":
                        t["query"] = t["query"].replace("/adverse effects", "")
                    if t["id"] == "32":
                        t["query"] = t["query"].replace("Ï", "I")
                    if t["id"] == "42":
                        t["query"] = t["query"].replace("/methods", "").replace("/standards", "")
                    if t["id"] == "51":
                        t["query"] = t["query"][:-2].replace("*Staphylococcus", "Staphylococcus")

                    f.write(Topic(identifier=t["id"],
                                  description=t["search_name"],
                                  raw_query=t["query"].replace("“", '"')
                                  .replace("”", '"')
                                  .replace("Atonic[tiab] Impaired[tiab]", 'Atonic[tiab] OR Impaired[tiab]')  # Covers topic 39.
                                  .replace("]/))", ']))')  # Covers topic 60.
                                  .replace("OR OR", 'OR'),  # Covers topic 51.
                                  date_from=datetime.strptime(t["Date_from"], "%d/%m/%Y").strftime("%Y/%m/%d"),
                                  date_to=datetime.strptime(t["Date_to"], "%d/%m/%Y").strftime("%Y/%m/%d")
                                  ).to_json() + "\n")

                with open(qrels_file, "a") as f:
                    for pmid in t["included_studies"]:
                        f.write(f'{t["id"]} 0 {pmid} 1\n')

    return Collection.from_dir(download_dir)


__collection_load_methods = {
    "ielab/sysrev-seed-collection": __load_sysrev_seed
}
