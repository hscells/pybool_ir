import os
from ftplib import FTP
from pathlib import Path

from pybool_ir import util
from pybool_ir.datasets.pubmed import datautils
from pybool_ir.datasets.pubmed.datautils import FTP_URL, FTP_BASELINE_CWD


# def download_baseline(path: Path):
#     with FTP(host=FTP_URL, user="anonymous") as ftp:
#         ftp.cwd(FTP_BASELINE_CWD)
#         files = []
#         ftp.dir(files.append)

#     os.makedirs(str(path), exist_ok=True)

#     for filename in reversed(datautils.dir_to_filenames(files)):
#         if os.path.exists(str(path / filename)):
#             print(f"found {path / filename}, skipping")
#             continue
#         util.download_file("https://" + FTP_URL + FTP_BASELINE_CWD + filename, path / filename)

#     ftp.close()

def download_baseline(path: Path, limit: int = None):
    with FTP(host=FTP_URL, user="anonymous") as ftp:
        ftp.cwd(FTP_BASELINE_CWD)
        files = []
        ftp.dir(files.append)

    os.makedirs(str(path), exist_ok=True)

    filenames = reversed(datautils.dir_to_filenames(files))

    if limit is not None and limit > 0: 
        filenames = list(filenames)[:limit]
        print(f"Limit set: Downloading first {limit} documents ...")

    for filename in filenames:
        if os.path.exists(str(path / filename)):
            print(f"found {path / filename}, skipping")
            continue
        
        util.download_file("https://" + FTP_URL + FTP_BASELINE_CWD  + filename, path / filename)
    
    ftp.close()