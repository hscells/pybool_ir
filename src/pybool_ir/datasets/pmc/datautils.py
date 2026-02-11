import os
from pathlib import Path
from typing import List

import requests

from pybool_ir.util import ProgressFile

FTP_URL = "ftp.ncbi.nlm.nih.gov"
FTP_PMC_CWD = "/pub/pmc/oa_bulk/"
FTP_OA_COMM_CWD = "oa_comm/xml/"
FTP_OA_NONCOMM_CWD = "oa_noncomm/xml/"
FTP_OA_OTHER_CWD = "oa_other/xml/"

MESH_YEAR = "2025"
MESH_URL = f"https://nlmpubs.nlm.nih.gov/projects/mesh/{MESH_YEAR}/meshtrees/"


def dir_to_filenames(listing: List[str]) -> List[str]:
    return [fname for fname in [x.split(" ")[-1] for x in listing] if fname.endswith(".gz") and "baseline" in fname]


