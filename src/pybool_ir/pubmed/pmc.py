import os
from ftplib import FTP
from pathlib import Path

from pybool_ir.pubmed import datautils
from pybool_ir.pubmed.datautils import FTP_URL, FTP_PMC_CWD


def download_baseline(path: Path):
    with FTP(host=FTP_URL, user="anonymous") as ftp:
        ftp.cwd(FTP_PMC_CWD)
        files = []
        ftp.dir(files.append)

    os.makedirs(str(path), exist_ok=True)

    for filename in reversed(datautils.dir_to_filenames(files)):
        if os.path.exists(str(path / filename)):
            print(f"found {path / filename}, skipping")
            continue
        # TODO: implement downloading PMC files.
        # datautils.download_single(path, filename)

    ftp.close()
