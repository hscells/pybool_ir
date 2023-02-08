"""
Utility functions for pybool_ir.
"""

import os
from io import BufferedRWPair, FileIO, BytesIO
from pathlib import Path
from typing import Union

import progressbar
import requests


class ProgressFile(BufferedRWPair):
    """
    This class opens a file for reading or writing, and when it is read
    or written to, it updates a progress bar to show how far along
    the reading or writing is.
    """

    # THANK YOU: https://stackoverflow.com/a/62792935
    def __init__(self, filename, mode="r", max_value=None):
        # noinspection PyArgumentList
        raw = FileIO(filename, mode=mode)

        # Do some magic that allows us to either read or write to the file stream.
        if mode == "r":
            # If mode is read, make an in-memory buffer to fake write to.
            super().__init__(raw, BytesIO())
        else:
            # Otherwise, we can fake read from memory.
            super().__init__(BytesIO(), raw)

        if mode == "r":
            self.length = os.stat(filename).st_size
            max_value = self.length
        self.max_value = max_value
        self.bar = progressbar.ProgressBar(
            widgets=[
                progressbar.Percentage(),
                progressbar.Bar(),
                str(filename),
                "|",
                progressbar.FileTransferSpeed(),
                "|",
                progressbar.ETA(),
            ],
            max_value=max_value,
        )
        self.written = 0  # Used only for write mode.

    def read(self, size=None):
        calc_sz = size
        if not calc_sz:
            calc_sz = self.length - self.tell()
        self._progress_callback(position=self.tell(), read_size=calc_sz)
        return super(ProgressFile, self).read(size)

    def write(self, b: Union[bytes, bytearray]) -> int:
        self._progress_callback(position=self.written, read_size=len(b))
        # We don't get `tell` in write mode, so can just calculate this way.
        self.written += len(b)
        return super(ProgressFile, self).write(b)

    def close(self) -> None:
        self.bar.finish()  # Formats the progress bar nicely at completion.
        return super(ProgressFile, self).close()

    def _progress_callback(self, position, read_size):
        if position + read_size > self.max_value:
            read_size = 0
            position = self.max_value
        self.bar.update(position + read_size)


def download_file(url: str, download_to: Path):
    """
    Helper function that downloads a file from a URL and shows a progress bar.
    """
    r = requests.get(url, stream=True, headers={'Accept-Encoding': None})
    size = int(r.headers.get("content-length"))
    with ProgressFile(download_to, "wb", max_value=size) as f:
        for chunk in r.iter_content(chunk_size=128):
            f.write(chunk)
