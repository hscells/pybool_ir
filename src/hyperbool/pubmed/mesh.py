import os
from pathlib import Path
from typing import List, Dict

import requests

from hyperbool.pubmed import datautils
from hyperbool.pubmed.datautils import MESH_YEAR, MESH_URL

DEFAULT_PATH = Path("./data/mesh")


class MeSHTree:
    def __init__(self, mtrees_file: Path = DEFAULT_PATH, year: str = MESH_YEAR):
        self.locations = {}
        self.headings: Dict[str, str] = {}
        with open(mtrees_file / f"mtrees{year}.bin", "r") as f:
            for line in f:
                heading, location = line.strip().split(";")
                self.locations[heading] = location.strip().replace("\n","")
                self.headings[location] = heading.strip().replace("\n","")

    def explode(self, heading: str) -> List[str]:
        location = self.locations[heading]
        for k, v in self.headings.items():
            if k.startswith(location):
                yield v


def download_mesh(path: Path = DEFAULT_PATH, year: str = MESH_YEAR) -> None:
    os.makedirs(str(path), exist_ok=True)
    remote_fname = f"mtrees{year}.bin"
    datautils.download_file(f"{MESH_URL}{remote_fname}", path / remote_fname)


def exists(path: Path = DEFAULT_PATH, year: str = MESH_YEAR) -> bool:
    return os.path.exists(path / f"mtrees{year}.bin")


if __name__ == '__main__':
    print(list(MeSHTree().explode("Anatomic Landmarks")))
