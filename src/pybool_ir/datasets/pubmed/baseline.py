import os
from ftplib import FTP
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor, as_completed

from pybool_ir import util
from pybool_ir.datasets.pubmed import datautils
from pybool_ir.datasets.pubmed.datautils import FTP_URL, FTP_BASELINE_CWD

def download_baseline(path: Path, limit: int = None, workers: int = 2, retries: int = 3):
    with FTP(host=FTP_URL, user="anonymous") as ftp:
        ftp.cwd(FTP_BASELINE_CWD)
        files = []
        ftp.dir(files.append)

    os.makedirs(str(path), exist_ok=True)
    filenames = list(reversed(datautils.dir_to_filenames(files)))

    if limit is not None and limit > 0:
        filenames = filenames[:limit]
        print(f"Limit set: Downloading first {limit} documents ...")

    def download_one(filename):
        target = path / filename
        if target.exists():
            return f"skip {filename}"

        url = "https://" + FTP_URL + FTP_BASELINE_CWD + filename

        for attempt in range(1, retries + 1):
            try:
                util.download_file(url, target)
                return f"done {filename}"
            except Exception as e:
                if attempt == retries:
                    return f"FAILED {filename}: {e}"
                time.sleep(2 * attempt)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(download_one, fn) for fn in filenames]
        for f in as_completed(futures):
            print(f.result())
