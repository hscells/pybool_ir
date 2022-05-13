import datetime
import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List

import appdirs
import ir_measures
from dataclasses_json import dataclass_json
from ir_measures import Qrel

from hyperbool import util

__GITHASH_CLEFTAR = "8ce8a63bebb7d88f42dc1abad3e5744e315d07ae"


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

        return Collection(str(collection_path).replace(str(Path(appdirs.user_data_dir("hyperbool")) / "collections"), "")[1:], topics, qrels)

    def __hash__(self):
        return hash("".join([repr(topic) for topic in self.topics] +
                            [repr(qrel) for qrel in self.qrels]))


def load_collection(name: str) -> Collection:
    return __collection_load_methods[name](name)


def parse_clef_tar_topic(topic_str: str, date_from: str = "1940", date_to: str = "2017", parse_query: bool = False) -> Topic:
    topic_id = ""
    topic_description = ""
    topic_query = ""
    parsing_query = False
    for line in topic_str.splitlines():
        if parse_query and parsing_query:
            if len(line) == 0:
                parsing_query = False
                continue
            topic_query += line
        if line.startswith("Topic:"):
            topic_id = line.replace("Topic: ", "")
        if line.startswith("Title:"):
            topic_description = line.replace("Title: ", "")
        if parse_query and line.startswith("Query:"):
            parsing_query = True

    # Don't worry, the pubdates will be added back in later.
    topic_query = topic_query.replace("Total references = 1551", "") \
        .replace("“", '"') \
        .replace("”", '"') \
        .replace(")* ", ') ') \
        .replace(" AND 1992/01/01:2015/11/30[crdt]", "") \
        .replace(" AND 1940/01/01:2012/09/21[crdt]", "") \
        .replace(" AND 1940/01/01:2015/02/28[crdt]", "") \
        .replace(" AND 1966/01/01:2017/03/30[crdt]", "") \
        .replace(" AND 1940/01/01:2016/01/19[crdt]", "")

    return Topic(identifier=topic_id.strip(),
                 description=topic_description,
                 raw_query=topic_query,
                 date_from=date_from,
                 date_to=date_to)


def __load_clef_tar_2017_training(name: str) -> Collection:
    return __load_clef_tar(name=name,
                           git_hash=__GITHASH_CLEFTAR,
                           year=2017,
                           subfolder="training",
                           qrels_path="qrels/train.abs.qrels",
                           topics_path="topics_train/",
                           topic_ids=["1", "11", "14", "19", "23", "28", "33", "35", "37", "38",
                                      "4", "43", "44", "45", "50", "53", "54", "55", "6", "9"],
                           pubmed_topic_ids=["19", "35", "37", "38", "44"])


def __load_clef_tar_2017_testing(name: str) -> Collection:
    return __load_clef_tar(name=name,
                           git_hash=__GITHASH_CLEFTAR,
                           year=2017,
                           subfolder="testing",
                           qrels_path="qrels/qrel_abs_test.txt",
                           topics_path="topics/",
                           topic_ids=["10", "12", "15", "16", "17", "18", "2", "21", "22", "25", "26",
                                      "27", "29", "31", "32", "34", "36", "39", "40", "41", "42", "47",
                                      "48", "49", "5", "51", "56", "57", "7", "8"],
                           pubmed_topic_ids=["32"])


def __load_clef_tar_2018_training(name: str) -> Collection:
    return __load_clef_tar(name=name,
                           git_hash=__GITHASH_CLEFTAR,
                           year=2018,
                           subfolder="Task2/Training/",
                           qrels_path="qrels/full.train.abs.2018.qrels",
                           topics_path="topics/",
                           topic_ids=["CD007394", "CD007427", "CD008054", "CD008081", "CD008643", "CD008686",
                                      "CD008691", "CD008760", "CD008782", "CD008803", "CD009020", "CD009135",
                                      "CD009185", "CD009323", "CD009372", "CD009519", "CD009551", "CD009579",
                                      "CD009591", "CD009593", "CD009647", "CD009786", "CD009925", "CD009944",
                                      "CD010023", "CD010173", "CD010276", "CD010339", "CD010386", "CD010409",
                                      "CD010438", "CD010542", "CD010632", "CD010633", "CD010653", "CD010705",
                                      "CD011134", "CD011548", "CD011549", "CD011975", "CD011984", "CD012019"],
                           pubmed_topic_ids=["CD009323", "CD010339", "CD011548", "CD011549", "CD008054", "CD009020"])


def __load_clef_tar_2018_testing(name: str) -> Collection:
    return __load_clef_tar(name=name,
                           git_hash=__GITHASH_CLEFTAR,
                           year=2018,
                           subfolder="Task2/Testing/",
                           qrels_path="qrels/full.test.abs.2018.qrels",
                           topics_path="topics/",
                           topic_ids=["CD008122", "CD008587", "CD008759", "CD008892", "CD009175", "CD009263",
                                      "CD009694", "CD010213", "CD010296", "CD010502", "CD010657", "CD010680",
                                      "CD010864", "CD011053", "CD011126", "CD011420", "CD011431", "CD011515",
                                      "CD011602", "CD011686", "CD011912", "CD011926", "CD012009", "CD012010",
                                      "CD012083", "CD012165", "CD012179", "CD012216", "CD012281", "CD012599"],
                           pubmed_topic_ids=["CD011420", "CD011912", "CD011926"],
                           pubdates_path="2018-TAR/Task1/Testing/pubdates.txt")


def __load_clef_tar(name: str, git_hash: str, year: int, subfolder: str,
                    qrels_path: str, topics_path: str, topic_ids: List[str],
                    pubmed_topic_ids: List[str] = None, pubdates_path: str = None) -> Collection:
    collection_base_url = f"https://raw.githubusercontent.com/CLEF-TAR/tar/{git_hash}/"
    collection_url = f"https://raw.githubusercontent.com/CLEF-TAR/tar/{git_hash}/{year}-TAR/{subfolder}/"
    qrels_url = collection_url + qrels_path

    download_dir = Path(appdirs.user_data_dir("hyperbool")) / "collections" / name
    raw_collection = download_dir / "raw"
    topic_file = download_dir / "topics.jsonl"
    qrels_file = download_dir / "qrels"

    pubdates = {}

    if not download_dir.exists():
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(raw_collection, exist_ok=True)
        util.download_file(qrels_url, qrels_file)

        if pubdates_path is not None:
            util.download_file(collection_base_url + pubdates_path, download_dir / "pubdates.txt")
            with open(download_dir / "pubdates.txt", "r") as f:
                for line in f:
                    topic, date_from, date_to = line.split()
                    pubdates[topic] = (f"{date_from[:4]}/{date_from[4:6]}/{date_from[6:]}",
                                       f"{date_to[:4]}/{date_to[4:6]}/{date_to[6:]}")

        with open(topic_file, "w") as f:
            for topic in topic_ids:
                date_from = "1940/01/01"
                date_to = f"{year}/01/01"

                if topic in pubdates:
                    date_from = pubdates[topic][0]
                    date_to = pubdates[topic][1]

                util.download_file(f"{collection_url}{topics_path}{topic}", raw_collection / topic)
                with open(raw_collection / topic, "r") as g:
                    f.write(parse_clef_tar_topic(topic_str=g.read(),
                                                 date_from=date_from,
                                                 date_to=date_to,
                                                 parse_query=topic in pubmed_topic_ids).to_json() + "\n")

    return Collection.from_dir(download_dir)


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
                    if t["id"] == "32":
                        t["query"] = t["query"].replace("Ï", "I")
                    if t["id"] == "51":
                        t["query"] = t["query"][:-2]

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
    # -------------------------------------------------------------------------------------------
    # For the sysrev-seed collection, we can use all the queries.
    "ielab/sysrev-seed-collection": __load_sysrev_seed,
    # -------------------------------------------------------------------------------------------
    # For CLEF TAR 2017 and 2018, the non-Pubmed queries are filtered out.
    "clef-tar/2017/training": __load_clef_tar_2017_training,
    "clef-tar/2017/testing": __load_clef_tar_2017_testing,
    "clef-tar/2018/training": __load_clef_tar_2018_training,
    "clef-tar/2018/testing": __load_clef_tar_2018_testing
    # -------------------------------------------------------------------------------------------
    # For CLEF TAR 2019, there are no additional topics that contain new Pubmed queries.
    # -------------------------------------------------------------------------------------------
    # Other possible datasets include:
    # - https://github.com/Amal-Alharbi/Systematic_Reviews_Update (but no Pubmed queries)
    # - https://github.com/ielab/SIGIR2017-SysRev-Collection (only about a dozen Pubmed queries from 125 in total)
    # -------------------------------------------------------------------------------------------

}
