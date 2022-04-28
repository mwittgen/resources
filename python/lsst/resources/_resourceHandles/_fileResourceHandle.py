# This file is part of lsst-resources.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# Use of this source code is governed by a 3-clause BSD-style
# license that can be found in the LICENSE file.
from __future__ import annotations

__all__ = ("FileResourceHandle", )

from io import SEEK_SET
from logging import Logger
from typing import AnyStr, Iterable, Optional, IO, TypeVar

from ._baseResourceHandle import BaseResourceHandle


U = TypeVar('U', str, bytes)


class FileResourceHandle(BaseResourceHandle[U]):
    """File based specialization of `BaseResourceHandle`

    Parameters
    ----------
    mode : `str`
        Handle modes as described in the python `io` module
    log : `~logging.Logger`
        Logger to used when writing messages
    lineseperator : `str`
        When doing multiline operations, break the stream on given character

    Notes
    -----
    Documentation on the methods of this class line should refer to the
    corresponding methods in the `io` module.
    """
    def __init__(self, mode: str, log: Logger, *, filename: str, encoding: Optional[str],
                 newline: str = '\n'):
        super().__init__(mode, log, newline=newline)
        self._filename = filename
        # opening a file in binary mode does not support a file argument
        if 'b' in mode:
            newline_arg = None
        else:
            newline_arg = newline
        self._fileHandle: IO = open(file=filename, mode=self._mode, newline=newline_arg, encoding=encoding)

    @property
    def mode(self) -> str:
        return self._mode

    def close(self) -> None:
        self._fileHandle.close()

    @property
    def closed(self) -> bool:
        return self._fileHandle.closed

    def fileno(self) -> int:
        return self._fileHandle.fileno()

    def flush(self) -> None:
        self._fileHandle.close()

    @property
    def isatty(self) -> bool:
        return self._fileHandle.isatty()

    def readable(self) -> bool:
        return self._fileHandle.readable()

    def readline(self, size: int = -1) -> AnyStr:
        return self._fileHandle.readline(size)
        ...

    def readlines(self, hint: int = -1) -> Iterable[AnyStr]:
        return self._fileHandle.readlines(hint)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        return self._fileHandle.seek(offset, whence)

    def seekable(self) -> bool:
        return self._fileHandle.seekable()

    def tell(self) -> int:
        return self._fileHandle.tell()

    def truncate(self, size: Optional[int] = None) -> int:
        return self._fileHandle.truncate()

    def writable(self) -> bool:
        return self._fileHandle.writable()

    def writelines(self, lines: Iterable[AnyStr]) -> None:
        self._fileHandle.writelines(lines)

    def read(self, size: int = -1) -> AnyStr:
        return self._fileHandle.read(size)

    def write(self, b: U) -> int:
        return self._fileHandle.write(b)
