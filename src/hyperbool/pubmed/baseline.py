import os
import sys
from ftplib import FTP
from pathlib import Path
from typing import List

import requests

from hyperbool.util import ProgressFile

FTP_URL = "ftp.ncbi.nlm.nih.gov"
FTP_CWD = "/pubmed/baseline/"


def dir_to_filenames(listing: List[str]) -> List[str]:
    return [fname for fname in [x.split(" ")[-1] for x in listing] if fname.endswith(".gz")]


def download_single(path: Path, filename: str):
    r = requests.get("https://" + FTP_URL + FTP_CWD + filename, stream=True)
    size = int(r.headers.get("content-length"))
    with ProgressFile(path / filename, "wb", max_value=size) as f:
        for chunk in r.iter_content(chunk_size=128):
            f.write(chunk)


def download_baseline(path: Path):
    with FTP(host=FTP_URL, user="anonymous") as ftp:
        ftp.cwd(FTP_CWD)
        files = []
        ftp.dir(files.append)

    os.makedirs(str(path), exist_ok=True)

    for filename in reversed(dir_to_filenames(files)):
        if os.path.exists(str(path / filename)):
            print(f"found {path / filename}, skipping")
            continue
        download_single(path, filename)

    ftp.close()


if __name__ == '__main__':
    download_baseline(Path(sys.argv[1]))
