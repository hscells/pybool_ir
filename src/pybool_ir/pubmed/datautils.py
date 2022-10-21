import os
from pathlib import Path
from typing import List

import requests

from pybool_ir.util import ProgressFile

FTP_URL = "ftp.ncbi.nlm.nih.gov"
FTP_BASELINE_CWD = "/pubmed/baseline/"
FTP_PMC_CWD = "/pubmed/baseline/"

MESH_YEAR = "2022"
MESH_URL = "https://nlmpubs.nlm.nih.gov/projects/mesh/MESH_FILES/meshtrees/"


def dir_to_filenames(listing: List[str]) -> List[str]:
    return [fname for fname in [x.split(" ")[-1] for x in listing] if fname.endswith(".gz")]


