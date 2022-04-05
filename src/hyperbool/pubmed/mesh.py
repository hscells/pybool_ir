import os
from pathlib import Path
from typing import List, Dict

import appdirs
import lucene
from lupyne import engine

from hyperbool import util
from hyperbool.pubmed.datautils import MESH_YEAR, MESH_URL

DEFAULT_PATH = Path(appdirs.user_data_dir("hyperbool")) / "data/mesh"

assert lucene.getVMEnv() or lucene.initVM()
analyzer = engine.analyzers.Analyzer.standard()


class MeSHTree:
    def __init__(self, mtrees_file: Path = DEFAULT_PATH, year: str = MESH_YEAR):
        self.locations = {}
        self.headings = []
        if not Path(mtrees_file / f"mtrees{year}.bin").exists():
            download_mesh()
        with open(mtrees_file / f"mtrees{year}.bin", "r") as f:
            for i, line in enumerate(f):  # Assumes all headings are sorted in order of location.
                heading, location = line.replace("\n", "").strip().split(";")
                # TODO: Need to index mesh headings in the same way.
                analyzed_heading = analyzer.parse(heading.
                                                  lower().
                                                  strip().
                                                  replace("-", " ").
                                                  replace("+", " ").
                                                  replace(")", " ").
                                                  replace("(", " ")).__str__()
                self.locations[analyzed_heading] = i
                self.headings.append((location.strip(), analyzed_heading))

    def explode(self, heading: str) -> List[str]:
        if heading.lower() not in self.locations:
            return []
        index = self.locations[heading.lower()]
        exploded_location, exploded_heading = self.headings[index]
        for indexed_heading in self.headings[index:]:
            location, heading = indexed_heading
            if location.startswith(exploded_location):
                yield heading
            else:
                break

def download_mesh(path: Path = DEFAULT_PATH, year: str = MESH_YEAR) -> None:
    os.makedirs(str(path), exist_ok=True)
    remote_fname = f"mtrees{year}.bin"
    util.download_file(f"{MESH_URL}{remote_fname}", path / remote_fname)


def exists(path: Path = DEFAULT_PATH, year: str = MESH_YEAR) -> bool:
    return os.path.exists(path / f"mtrees{year}.bin")
