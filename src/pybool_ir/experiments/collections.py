import datetime
import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List

import appdirs
import ir_measures
from dataclasses_json import dataclass_json
from ir_measures import Qrel

from pybool_ir import util
from pybool_ir.query import ovid

_GITHASH_CLEFTAR = "8ce8a63bebb7d88f42dc1abad3e5744e315d07ae"
_package_name = "pybool_ir"
_base_dir = Path(appdirs.user_data_dir(_package_name))


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

        return Collection(str(collection_path).replace(str(_base_dir / "collections"), "")[1:], topics, qrels)

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
            topic_query += line + "\n"
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
                           git_hash=_GITHASH_CLEFTAR,
                           year=2017,
                           subfolder="training",
                           qrels_path="qrels/train.abs.qrels",
                           topics_path="topics_train/",
                           topic_ids=["1", "11", "14", "19", "23", "28", "33", "35", "37", "38",
                                      "4", "43", "44", "45", "50", "53", "54", "55", "6", "9"],
                           pubmed_topic_ids=["19", "35", "37", "38", "44"])


def __load_clef_tar_2017_testing(name: str) -> Collection:
    return __load_clef_tar(name=name,
                           git_hash=_GITHASH_CLEFTAR,
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
                           git_hash=_GITHASH_CLEFTAR,
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
                           git_hash=_GITHASH_CLEFTAR,
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

    download_dir = _base_dir / "collections" / name
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


def __load_wang_clef(name: str, year: int) -> Collection:
    git_hash = "7a23a14ced14021f4db910f83330426b07ce3e5c"
    collection_url = f"https://raw.githubusercontent.com/ielab/meshsuggest/{git_hash}/queries_new/original_full_query/{year}/testing/"

    download_dir = _base_dir / "collections" / name
    tar_download_dir = _base_dir / "collections" / "clef-tar" / str(year) / "testing"
    tar_qrels = tar_download_dir / "qrels"
    tar_topics = tar_download_dir / "topics.jsonl"
    tar_raw = tar_download_dir / "raw"
    raw_collection = download_dir / "raw"
    topic_file = download_dir / "topics.jsonl"
    qrels_file = download_dir / "qrels"

    if not download_dir.exists():
        os.makedirs(download_dir, exist_ok=True)
        os.makedirs(raw_collection, exist_ok=True)
        shutil.copyfile(tar_qrels, qrels_file)

        with open(tar_topics, "r") as tar_f:
            with open(topic_file, "w") as shuai_f:
                for line in tar_f:
                    topic = Topic.from_json(line)

                    util.download_file(f"{collection_url}{topic.identifier}", raw_collection / topic.identifier)
                    with open(raw_collection / topic.identifier, "r") as g:
                        query = g.read()
                        if query == "404: Not Found":
                            if len(topic.raw_query) > 0:
                                query = topic.raw_query
                            else:
                                for raw_file in os.listdir(tar_raw):
                                    with open(tar_raw / str(raw_file), "r") as raw_f:
                                        if topic.identifier in raw_f.read():
                                            mapped_id = str(raw_file)
                                with open(tar_raw / mapped_id, "r") as topic_f:
                                    topic_q = parse_clef_tar_topic(topic_f.read(), parse_query=True)
                                    topic_q.raw_query = topic_q.raw_query. \
                                        replace("77679-27-7[rn]", ""). \
                                        replace("limit 3 to humans", ""). \
                                        replace("limit 4 to ed=19920101-20120831", "")
                                    topic_q.raw_query = "\n".join([line for line in topic_q.raw_query.split("\n") if line.strip() != ''])
                                    if len(topic_q.raw_query.split("\n")) > 2:
                                        query = ovid.transform(topic_q.raw_query)
                                    else:
                                        query = topic_q.raw_query

                        if topic.identifier == "CD008122" or topic.identifier == "CD011431":
                            query = query + ")"
                        if topic.identifier == "CD009263":
                            query = "(" + query
                            query = query.replace("""((mibg OR iodine-123 metaiodobenzylguanidine imaging OR iodine-123 metaiodobenzylguanidine Imag* OR metaiodobenzyl guanidine OR Metaiodobenzylguanidin* OR metaiodobenzylguanidine scintigraphy OR metaiodobenzylguanidine scintigraph*) OR (123I-mIBG) OR (3 iodobenzylguanidine OR meta-iodobenzylguanidine OR meta iodobenzylguanidine OR iobenguane OR m iodobenzylguanidine OR m iodobenzylguanidine OR (iobenguane AND (131I) OR (3-IodoND (131I) AND benzyl) AND guanidine) OR 3-iodobenzylguanidine, 123i labeled OR 123i labeled 3-iodobenzylguanidine OR 3 iodobenzylguanidine, 123i labeled OR meta-iodobenzylguanidine OR meta iodobenzylguanidine OR m-iodobenzylguanidine OR m iodobenzylguanidine OR iobenguane (131I) OR (3-Iodo131I) benzyl) guanidine)""",
                                                  """(mibg OR iodine-123 metaiodobenzylguanidine imaging OR iodine-123 metaiodobenzylguanidine Imag* OR metaiodobenzyl guanidine OR Metaiodobenzylguanidin* OR metaiodobenzylguanidine scintigraphy OR metaiodobenzylguanidine scintigraph*) OR (123I-mIBG) OR (3 iodobenzylguanidine OR meta-iodobenzylguanidine OR meta iodobenzylguanidine OR iobenguane OR m iodobenzylguanidine OR m iodobenzylguanidine OR (iobenguane AND ((131I OR 3-IodoND 131I) AND benzyl) AND guanidine) OR 3-iodobenzylguanidine, 123i labeled OR 123i labeled 3-iodobenzylguanidine OR 3 iodobenzylguanidine, 123i labeled OR meta-iodobenzylguanidine OR meta iodobenzylguanidine OR m-iodobenzylguanidine OR m iodobenzylguanidine OR iobenguane 131I OR 3-Iodo131I benzyl guanidine)""")

                        query = query.replace("""tomography[MeSH Terms] tomography, optical coherence[MeSH Terms]""",
                                              """tomography[MeSH Terms] OR tomography, optical coherence[MeSH Terms]""")
                        query = query.replace("OR ()", "")
                        query = query.replace('"Diagnostic and Statistical Manual of Mental Disorders', '"Diagnostic and Statistical Manual of Mental Disorders"')

                        shuai_f.write(Topic(identifier=topic.identifier,
                                            description=topic.description,
                                            date_from=topic.date_from,
                                            date_to=topic.date_to,
                                            raw_query=query).to_json() + "\n")
    return Collection.from_dir(download_dir)


def __load_wang_clef_tar_2017_testing(name: str):
    __load_clef_tar_2017_testing("clef-tar/2017/testing")
    return __load_wang_clef(name, 2017)


def __load_wang_clef_tar_2018_testing(name: str):
    __load_clef_tar_2018_testing("clef-tar/2018/testing")
    return __load_wang_clef(name, 2018)


def __load_sysrev_seed(name: str) -> Collection:
    git_hash = "c40598fc2ad8c7dea8840681de3386f78869d77a"
    collection_url = f"https://github.com/ielab/sysrev-seed-collection/raw/{git_hash}/collection_data/overall_collection.jsonl"
    download_dir = _base_dir / "collections" / name
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

                    f.write(Topic(identifier=t["id"],
                                  description=t["title"],
                                  raw_query=t["query"] \
                                  .replace('("Cochrane Database Syst Rev"[journal])', "")
                                  .replace("“", '"')
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


def __load_update_collection(name: str) -> Collection:
    import pickle
    import zipfile

    git_hash = "35a78b615d9c9dbdd889c55a61e5032b3cc309c6"
    collection_url = f"https://github.com/Amal-Alharbi/Systematic_Reviews_Update/raw/{git_hash}/update_dataset.pkl.zip"
    download_dir = _base_dir / "collections" / name
    pickle_collection = download_dir / "update_dataset.pkl"
    pickle_collection_zip = download_dir / "update_dataset.pkl.zip"

    print(download_dir)

    if not download_dir.exists():
        os.makedirs(download_dir, exist_ok=True)
        util.download_file(collection_url, pickle_collection_zip)

        with zipfile.ZipFile(pickle_collection_zip, "r") as z:
            z.extractall(download_dir)

    with open(pickle_collection, "rb") as f:
        reviews = pickle.load(f)

    # TODO When unpickling, there is a ModuleNotFoundError.

    raise Exception("Not yet implemented")


__collection_load_methods = {
    # -------------------------------------------------------------------------------------------
    # For the sysrev-seed collection, we can use all the queries.
    "ielab/sysrev-seed-collection": __load_sysrev_seed,
    # -------------------------------------------------------------------------------------------
    # For CLEF TAR 2017 and 2018, the non-Pubmed queries are filtered out.
    "clef-tar/2017/training": __load_clef_tar_2017_training,
    "clef-tar/2017/testing": __load_clef_tar_2017_testing,
    "clef-tar/2018/training": __load_clef_tar_2018_training,
    "clef-tar/2018/testing": __load_clef_tar_2018_testing,
    # -------------------------------------------------------------------------------------------
    # For some work on MeSH Term suggestion, the CLEF TAR queries have been translated to Pubmed.
    # NOTE: Only testing queries have been translated.
    "wang/clef-tar/2017/testing": __load_wang_clef_tar_2017_testing,
    "wang/clef-tar/2018/testing": __load_wang_clef_tar_2018_testing,
    # -------------------------------------------------------------------------------------------
    # For CLEF TAR 2019, there are no additional topics that contain new Pubmed queries.
    # -------------------------------------------------------------------------------------------
    # Other possible datasets include:
    # - https://github.com/Amal-Alharbi/Systematic_Reviews_Update (but no Pubmed queries)
    "amal-alharbi/systematic-reviews-update": __load_update_collection
    # - https://github.com/ielab/SIGIR2017-SysRev-Collection (only about a dozen Pubmed queries from 125 in total)
    # -------------------------------------------------------------------------------------------

}
