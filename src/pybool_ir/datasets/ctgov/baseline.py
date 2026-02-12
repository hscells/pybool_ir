from pathlib import Path
import zipfile
import wget

from pybool_ir.datasets.ctgov.datautils import CTGOV_URL

def download_baseline(path: Path):
    wget.download(CTGOV_URL)
    with zipfile.ZipFile("ctg-public-xml.zip", "r") as f:
        f.extractall(path)
    
