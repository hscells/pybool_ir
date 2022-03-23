import os
from pathlib import Path
from typing import List

import requests

from hyperbool.util import ProgressFile

FTP_URL = "ftp.ncbi.nlm.nih.gov"
FTP_BASELINE_CWD = "/pubmed/baseline/"
FTP_PMC_CWD = "/pubmed/baseline/"

MESH_YEAR = "2022"
MESH_URL = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/meshtrees/"


def dir_to_filenames(listing: List[str]) -> List[str]:
    return [fname for fname in [x.split(" ")[-1] for x in listing] if fname.endswith(".gz")]


def download_file(url: str, download_to: Path):
    r = requests.get(url, stream=True)
    size = int(r.headers.get("content-length"))
    with ProgressFile(download_to, "wb", max_value=size) as f:
        for chunk in r.iter_content(chunk_size=128):
            f.write(chunk)
