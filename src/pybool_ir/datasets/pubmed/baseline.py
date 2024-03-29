import os
from ftplib import FTP
from pathlib import Path

from pybool_ir import util
from pybool_ir.datasets.pubmed import datautils
from pybool_ir.datasets.pubmed.datautils import FTP_URL, FTP_BASELINE_CWD


def download_baseline(path: Path):
    with FTP(host=FTP_URL, user="anonymous") as ftp:
        ftp.cwd(FTP_BASELINE_CWD)
        files = []
        ftp.dir(files.append)

    os.makedirs(str(path), exist_ok=True)

    for filename in reversed(datautils.dir_to_filenames(files)):
        if os.path.exists(str(path / filename)):
            print(f"found {path / filename}, skipping")
            continue
        util.download_file("https://" + FTP_URL + FTP_BASELINE_CWD + filename, path / filename)

    ftp.close()
