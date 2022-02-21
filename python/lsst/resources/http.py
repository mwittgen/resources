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

import functools
import logging
import os
import os.path
import random
import stat
import tempfile

import requests

__all__ = ("HttpResourcePath",)

from typing import TYPE_CHECKING, Optional, Tuple, Union

from lsst.utils.timer import time_this
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from urllib3.util.retry import Retry

from ._resourcePath import ResourcePath

if TYPE_CHECKING:
    from .utils import TransactionProtocol

log = logging.getLogger(__name__)


# Default timeouts for all HTTP requests, in seconds
DEFAULT_TIMEOUT_CONNECT = 60
DEFAULT_TIMEOUT_READ = 300

# Allow for network timeouts to be set in the environment
TIMEOUT = (
    int(os.environ.get("LSST_HTTP_TIMEOUT_CONNECT", DEFAULT_TIMEOUT_CONNECT)),
    int(os.environ.get("LSST_HTTP_TIMEOUT_READ", DEFAULT_TIMEOUT_READ)),
)


def _get_http_session(path: ResourcePath, persist: bool = True) -> requests.Session:
    """Create a requests.Session pre-configured with environment variable data.

    Parameters
    ----------
    path : `ResourcePath`
        URL to a resource in the remote server for which the session is to be
        created

    persist: `bool`
        if `True`, persist the connection with the front end server.
        In any case, connections to the backend servers are not persisted.

    Returns
    -------
    session : `requests.Session`
        An http session used to execute requests.

    Notes
    -----
    The following environment variables are inspected:
    - LSST_HTTP_CACERT_BUNDLE: path to a .pem file containing the CA
        certificates to trust when verifying the server's certificate.
    - LSST_HTTP_AUTH_BEARER_TOKEN: value of a bearer token or path to a local
        file containing a bearer token to be used as the client authentication
        mechanism with all requests.
        The permissions of the token file must be set so that only its owner
        can access it.
        If initialized, takes precedence over LSST_HTTP_AUTH_CLIENT_CERT and
        LSST_HTTP_AUTH_CLIENT_KEY.
    - LSST_HTTP_AUTH_CLIENT_CERT: path to a .pem file which contains the client
        certificate for authenticating to the server.
        If initialized, the variable LSST_HTTP_AUTH_CLIENT_KEY must also be
        initialized with the path of the client private key file.
        The permissions of the client private key must be set so that only
        its owner can access it.
    - LSST_HTTP_PUT_SEND_EXPECT_HEADER: if set, a "Expect: 100-Continue"
        header will be added to all HTTP PUT requests.
        This header is required by some servers to detect if the client knows
        how to handle redirections. In case of redirection, the body of the
        PUT request is sent to the redirected location.
    """
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=5.0 + random.random(),
        status=3,
        status_forcelist=[429, 500, 502, 503, 504],
    )

    session = requests.Session()
    root_uri = str(path.root_uri())
    log.debug("Creating new HTTP session for endpoint %s (persist connection=%s)...", root_uri, persist)

    # Mount an HTTP adapter to prevent persisting connections to back-end
    # servers which may vary from request to request. Systematically persisting
    # connections to those servers may exhaust their capabilities when there
    # are thousands of simultaneous clients
    session.mount(
        f"{path.scheme}://",
        HTTPAdapter(pool_connections=1, pool_maxsize=0, pool_block=False, max_retries=retries),
    )
    # Persist a single connection to the front end server, if required
    num_connections = 1 if persist else 0
    session.mount(
        root_uri,
        HTTPAdapter(pool_connections=1, pool_maxsize=num_connections, pool_block=False, max_retries=retries),
    )

    # Should we use a specific CA cert bundle for authenticating the server?
    session.verify = True
    if ca_bundle := os.getenv("LSST_HTTP_CACERT_BUNDLE"):
        session.verify = ca_bundle
    else:
        log.debug(
            "Environment variable LSST_HTTP_CACERT_BUNDLE is not set: "
            "if you would need to verify the remote server's certificate "
            "issued by specific certificate authorities please consider "
            "initializing this variable."
        )

    # Should we use bearer tokens for client authentication?
    if token := os.getenv("LSST_HTTP_AUTH_BEARER_TOKEN"):
        log.debug("... using bearer token authentication")
        session.auth = BearerTokenAuth(token)
        return session

    # Should we instead use client certificate and private key? If so, both
    # LSST_HTTP_AUTH_CLIENT_CERT and LSST_HTTP_AUTH_CLIENT_KEY must be
    # initialized
    client_cert = os.getenv("LSST_HTTP_AUTH_CLIENT_CERT")
    client_key = os.getenv("LSST_HTTP_AUTH_CLIENT_KEY")
    if client_cert and client_key:
        if not _is_protected(client_key):
            raise PermissionError(
                f"Private key file at {client_key} must be protected for access only by its owner"
            )
        log.debug("... using client certificate authentication.")
        session.cert = (client_cert, client_key)
        return session

    if client_cert:
        # Only the client certificate was provided
        raise ValueError(
            "Environment variable LSST_HTTP_AUTH_CLIENT_KEY must be set to client private key file path"
        )

    if client_key:
        # Only the client private key was provided
        raise ValueError(
            "Environment variable LSST_HTTP_AUTH_CLIENT_CERT must be set to client certificate file path"
        )

    log.warning(
        "Neither LSST_HTTP_AUTH_BEARER_TOKEN nor (LSST_HTTP_AUTH_CLIENT_CERT and LSST_HTTP_AUTH_CLIENT_KEY)"
        " are initialized. No client authentication enabled."
    )
    return session


@functools.lru_cache
def _send_expect_header_on_put() -> bool:
    """Return true if HTTP PUT requests should include the
    'Expect: 100-continue' header.

    Returns
    -------
    _send_expect_header_on_put : `bool`
        True if LSST_HTTP_PUT_SEND_EXPECT_HEADER is set, False otherwise.
    """
    # The 'Expect: 100-continue' header is used by some servers (e.g. dCache)
    # as an indication that the client knows how to handle redirects to
    # the specific server that will receive the data when doing of PUT
    # requests.
    return "LSST_HTTP_PUT_SEND_EXPECT_HEADER" in os.environ


@functools.lru_cache
def isWebdavEndpoint(path: Union[ResourcePath, str]) -> bool:
    """Check whether the remote HTTP endpoint implements WebDAV features.

    Parameters
    ----------
    path : `ResourcePath` or `str`
        URL to the resource to be checked.
        Should preferably refer to the root since the status is shared
        by all paths in that server.

    Returns
    -------
    isWebdavEndpoint : `bool`
        True if the endpoint implements WebDAV, False if it doesn't.
    """
    ca_bundle = True
    try:
        ca_bundle = os.environ["LSST_HTTP_CACERT_BUNDLE"]
    except KeyError:
        log.warning(
            "Environment variable LSST_HTTP_CACERT_BUNDLE is not set: "
            "some HTTPS requests may fail if remote server presents a "
            "certificate issued by an unknown certificate authority. "
        )

    log.debug("Detecting HTTP endpoint type for '%s'...", path)
    r = requests.options(str(path), verify=ca_bundle)
    return "DAV" in r.headers


# Tuple (path, block_size) pointing to the location of a local directory
# to save temporary files and the block size of the underlying file system
_TMPDIR: Optional[Tuple[str, int]] = None


def _get_temp_dir() -> Tuple[str, int]:
    """Return the temporary directory path and block size.

    This function caches its results in _TMPDIR.
    """
    global _TMPDIR
    if _TMPDIR:
        return _TMPDIR

    # Use the value of environment variables 'LSST_RESOURCES_TMPDIR' or
    # 'TMPDIR', if defined. Otherwise use current working directory
    tmpdir = os.getcwd()
    for dir in (os.getenv(v) for v in ("LSST_RESOURCES_TMPDIR", "TMPDIR")):
        if dir and os.path.isdir(dir):
            tmpdir = dir
            break

    # Compute the block size as 256 blocks of typical size
    # (i.e. 4096 bytes) or 10 times the file system block size,
    # whichever is higher. This is a reasonable compromise between
    # using memory for buffering and the number of system calls
    # issued to read from or write to temporary files
    fsstats = os.statvfs(tmpdir)
    return (_TMPDIR := (tmpdir, max(10 * fsstats.f_bsize, 256 * 4096)))


class BearerTokenAuth(AuthBase):
    """Attach a bearer token Authorization header to request"""

    def __init__(self, token: str):
        # token may be the token value itself or a path to a file containing
        # the token value. The file must be protected so that only its owner
        # can access it
        self._token = self._path = None
        self._mtime = -1
        if not token:
            return
        self._token = token
        if os.path.isfile(token):
            self._path = token
            if not _is_protected(self._path):
                raise PermissionError(
                    f"Bearer token file at {self._path} must be protected for access only by its owner"
                )
            self._refresh()

    def _refresh(self) -> None:
        # Read the token file (if any) if its modification time is more recent
        # than the the last time we read it
        if not self._path:
            return

        if (mtime := os.stat(self._path).st_mtime) > self._mtime:
            log.debug("Reading bearer token file at %s", self._path)
            self._mtime = mtime
            with open(self._path) as f:
                self._token = f.read().rstrip("\n")

    def __call__(self, req: requests.Request) -> requests.Request:
        if self._token:
            self._refresh()
            req.headers["Authorization"] = f"Bearer {self._token}"
        return req


class HttpResourcePath(ResourcePath):
    """General HTTP(S) resource."""

    _is_webdav: Optional[bool] = None

    # Use a session exclusively for PUT requests and another session for
    # all other requests. PUT requests may be redirected and in that case
    # the server may close the persisted connection. If that is the case
    # only the connection persisted for PUT requests will be closed and
    # the other persisted connection will be kept alive and reused for
    # other requests.
    _session: Optional[requests.Session] = None
    _upload_session: Optional[requests.Session] = None

    @property
    def session(self) -> requests.Session:
        """Client object to address remote resource."""
        cls = type(self)
        if cls._session:
            return cls._session

        cls._session = _get_http_session(self)
        return cls._session

    @property
    def upload_session(self) -> requests.Session:
        """Client object to address remote resource for PUT requests."""
        cls = type(self)
        if cls._upload_session:
            return cls._upload_session

        log.debug("Creating new HTTP session for PUT requests: %s", self.geturl())
        cls._upload_session = _get_http_session(self)
        return cls._upload_session

    @property
    def is_webdav_endpoint(self) -> bool:
        """Check if the current endpoint implements WebDAV features.

        This is stored per URI but cached by root so there is
        only one check per hostname.
        """
        if self._is_webdav is not None:
            return self._is_webdav

        self._is_webdav = isWebdavEndpoint(self.root_uri())
        return self._is_webdav

    def exists(self) -> bool:
        """Check that a remote HTTP resource exists."""
        log.debug("Checking if resource exists: %s", self.geturl())
        resp = self.session.head(self.geturl(), timeout=TIMEOUT)

        return resp.status_code == 200

    def size(self) -> int:
        """Return the size of the remote resource in bytes."""
        if self.dirLike:
            return 0
        resp = self.session.head(self.geturl(), timeout=TIMEOUT)
        if resp.status_code == 200:
            return int(resp.headers["Content-Length"])
        else:
            raise FileNotFoundError(f"Resource {self} does not exist")

    def mkdir(self) -> None:
        """Create the directory resource if it does not already exist."""
        # Only available on WebDAV backends
        if not self.is_webdav_endpoint:
            raise NotImplementedError("Endpoint does not implement WebDAV functionality")

        if not self.dirLike:
            raise ValueError(f"Can not create a 'directory' for file-like URI {self}")

        if not self.exists():
            # We need to test the absence of the parent directory,
            # but also if parent URL is different from self URL,
            # otherwise we could be stuck in a recursive loop
            # where self == parent
            if not self.parent().exists() and self.parent().geturl() != self.geturl():
                self.parent().mkdir()
            log.debug("Creating new directory: %s", self.geturl())
            resp = self.session.request("MKCOL", self.geturl(), timeout=TIMEOUT)
            if resp.status_code != 201:
                if resp.status_code == 405:
                    log.debug("Can not create directory: %s may already exist: skipping.", self.geturl())
                else:
                    raise ValueError(f"Can not create directory {self}, status code: {resp.status_code}")

    def remove(self) -> None:
        """Remove the resource."""
        log.debug("Removing resource: %s", self.geturl())
        resp = self.session.delete(self.geturl(), timeout=TIMEOUT)
        if resp.status_code not in [200, 202, 204]:
            raise FileNotFoundError(f"Unable to delete resource {self}; status code: {resp.status_code}")

    def _as_local(self) -> Tuple[str, bool]:
        """Download object over HTTP and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        resp = self.session.get(self.geturl(), stream=True, timeout=TIMEOUT)
        if resp.status_code != 200:
            raise FileNotFoundError(f"Unable to download resource {self}; status code: {resp.status_code}")

        tmpdir, buffering = _get_temp_dir()
        with tempfile.NamedTemporaryFile(
            suffix=self.getExtension(), buffering=buffering, dir=tmpdir, delete=False
        ) as tmpFile:
            with time_this(
                log,
                msg="Downloading %s [length=%s] to local file %s [chunk_size=%d]",
                args=(self, resp.headers.get("Content-Length"), tmpFile.name, buffering),
            ):
                for chunk in resp.iter_content(chunk_size=buffering):
                    tmpFile.write(chunk)
        return tmpFile.name, True

    def read(self, size: int = -1) -> bytes:
        """Open the resource and return the contents in bytes.

        Parameters
        ----------
        size : `int`, optional
            The number of bytes to read. Negative or omitted indicates
            that all data should be read.
        """
        log.debug("Reading from remote resource: %s", self.geturl())
        stream = True if size > 0 else False
        with time_this(log, msg="Read from remote resource %s", args=(self,)):
            resp = self.session.get(self.geturl(), stream=stream, timeout=TIMEOUT)
        if resp.status_code != 200:
            raise FileNotFoundError(f"Unable to read resource {self}; status code: {resp.status_code}")
        if not stream:
            return resp.content
        else:
            return next(resp.iter_content(chunk_size=size))

    def write(self, data: bytes, overwrite: bool = True) -> None:
        """Write the supplied bytes to the new resource.

        Parameters
        ----------
        data : `bytes`
            The bytes to write to the resource. The entire contents of the
            resource will be replaced.
        overwrite : `bool`, optional
            If `True` the resource will be overwritten if it exists. Otherwise
            the write will fail.
        """
        log.debug("Writing to remote resource: %s", self.geturl())
        if not overwrite:
            if self.exists():
                raise FileExistsError(f"Remote resource {self} exists and overwrite has been disabled")
        with time_this(log, msg="Write to remote %s (%d bytes)", args=(self, len(data))):
            self._do_put(data=data)

    def transfer_from(
        self,
        src: ResourcePath,
        transfer: str = "copy",
        overwrite: bool = False,
        transaction: Optional[TransactionProtocol] = None,
    ) -> None:
        """Transfer the current resource to a Webdav repository.

        Parameters
        ----------
        src : `ResourcePath`
            Source URI.
        transfer : `str`
            Mode to use for transferring the resource. Supports the following
            options: copy.
        transaction : `~lsst.resources.utils.TransactionProtocol`, optional
            Currently unused.
        """
        # Fail early to prevent delays if remote resources are requested
        if transfer not in self.transferModes:
            raise ValueError(f"Transfer mode {transfer} not supported by URI scheme {self.scheme}")

        # Existence checks cost time so do not call this unless we know
        # that debugging is enabled.
        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                "Transferring %s [exists: %s] -> %s [exists: %s] (transfer=%s)",
                src,
                src.exists(),
                self,
                self.exists(),
                transfer,
            )

        # Short circuit if the URIs are identical immediately.
        if self == src:
            log.debug(
                "Target and destination URIs are identical: %s, returning immediately."
                " No further action required.",
                self,
            )
            return

        if self.exists() and not overwrite:
            raise FileExistsError(f"Destination path {self} already exists.")

        if transfer == "auto":
            transfer = self.transferDefault

        if isinstance(src, type(self)):
            # Only available on WebDAV backends
            if not self.is_webdav_endpoint:
                raise NotImplementedError("Endpoint does not implement WebDAV functionality")

            with time_this(log, msg="Transfer from %s to %s directly", args=(src, self)):
                method = "MOVE" if transfer == "move" else "COPY"
                log.debug("%s from %s to %s", method, src.geturl(), self.geturl())
                resp = self.session.request(
                    method, src.geturl(), headers={"Destination": self.geturl()}, timeout=TIMEOUT
                )
                if resp.status_code not in [201, 202, 204]:
                    raise ValueError(f"Can not transfer file {self}, status code: {resp.status_code}")
        else:
            # Use local file and upload it
            with src.as_local() as local_uri:
                with open(local_uri.ospath, "rb") as f:
                    with time_this(log, msg="Transfer from %s to %s via local file", args=(src, self)):
                        self._do_put(data=f)

            # This was an explicit move requested from a remote resource
            # try to remove that resource
            if transfer == "move":
                # Transactions do not work here
                src.remove()

    def _do_put(self, data) -> None:
        """Perform an HTTP PUT request taking into account redirection"""
        final_url = self.geturl()
        if _send_expect_header_on_put():
            # Do a PUT request with an empty body and retrieve the final
            # destination URL returned by the server
            headers = {"Content-Length": "0", "Expect": "100-continue"}
            resp = self.upload_session.put(
                final_url, data=None, headers=headers, allow_redirects=False, timeout=TIMEOUT
            )
            if resp.is_redirect or resp.is_permanent_redirect:
                final_url = resp.headers["Location"]
                log.debug("PUT request to %s redirected to %s", self.geturl(), final_url)

        # Send data to its final destination
        resp = self.upload_session.put(final_url, data=data, timeout=TIMEOUT)
        if resp.status_code not in [201, 202, 204]:
            raise ValueError(f"Can not write file {self}, status code: {resp.status_code}")


def _is_protected(filepath: str) -> bool:
    """Return true if the permissions of file at filepath only allow for access
    by its owner
    """
    if not os.path.isfile(filepath):
        return False
    mode = stat.S_IMODE(os.stat(filepath).st_mode)
    owner_accessible = mode & stat.S_IRWXU
    group_accessible = mode & stat.S_IRWXG
    other_accessible = mode & stat.S_IRWXO
    return owner_accessible and not group_accessible and not other_accessible
