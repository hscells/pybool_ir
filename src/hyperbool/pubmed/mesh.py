import os
from pathlib import Path
from typing import List, Dict

import lucene
from lupyne import engine

from hyperbool.pubmed import datautils
from hyperbool.pubmed.datautils import MESH_YEAR, MESH_URL

DEFAULT_PATH = Path("./data/mesh")

assert lucene.getVMEnv() or lucene.initVM()
analyzer = engine.analyzers.Analyzer.standard()


class MeSHTree:
    def __init__(self, mtrees_file: Path = DEFAULT_PATH, year: str = MESH_YEAR):
        self.locations = {}
        self.headings: Dict[str, str] = {}
        with open(mtrees_file / f"mtrees{year}.bin", "r") as f:
            for line in f:
                heading, location = line.replace("\n", "").strip().split(";")
                # TODO: Need to index mesh headings in the same way.
                analyzed_heading = analyzer.parse(heading.
                                                  lower().
                                                  strip().
                                                  replace("-", " ").
                                                  replace("+", " ").
                                                  replace(")", " ").
                                                  replace("(", " ")).__str__()
                self.locations[analyzed_heading] = location.strip()
                self.headings[location] = analyzed_heading

    def explode(self, heading: str) -> List[str]:
        if heading.lower() not in self.locations:
            return []
        location = self.locations[heading.lower()]
        for k, v in self.headings.items():
            # All child terms, but not including the original term.
            if k.startswith(location) and location != k:
                yield v


def download_mesh(path: Path = DEFAULT_PATH, year: str = MESH_YEAR) -> None:
    os.makedirs(str(path), exist_ok=True)
    remote_fname = f"mtrees{year}.bin"
    datautils.download_file(f"{MESH_URL}{remote_fname}", path / remote_fname)


def exists(path: Path = DEFAULT_PATH, year: str = MESH_YEAR) -> bool:
    return os.path.exists(path / f"mtrees{year}.bin")


if __name__ == '__main__':
    print(list(MeSHTree().explode("Anatomic Landmarks")))
