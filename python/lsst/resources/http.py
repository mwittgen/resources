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
import tempfile

import requests

__all__ = ("HttpResourcePath",)

from typing import TYPE_CHECKING, Optional, Tuple, Union

from lsst.utils.timer import time_this
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ._resourcePath import ResourcePath

if TYPE_CHECKING:
    from .utils import TransactionProtocol

log = logging.getLogger(__name__)

# Default timeout for all HTTP requests, in seconds
TIMEOUT = 20


def getHttpSession() -> requests.Session:
    """Create a requests.Session pre-configured with environment variable data.

    Returns
    -------
    session : `requests.Session`
        An http session used to execute requests.

    Notes
    -----
    The following environment variables are inspected:
    - LSST_HTTP_CACERT_BUNDLE: a .pem file containing the CA certificates
        to trust when verifying the server's certificate.
    - LSST_HTTP_AUTH_BEARER_TOKEN: path to a (protected) file containing
        a bearer token to be used with all requests. If initialized, takes
        precedence over LSST_HTTP_AUTH_CLIENT_CERT and LSST_HTTP_AUTH_CLIENT_KEY.
    - LSST_HTTP_AUTH_CLIENT_CERT: path to the client certificate to use for
        authenticating to the server. If initialized, the variable
        LSST_HTTP_AUTH_CLIENT_KEY must also be initialized with the path of
        the client certificate private key file, which should be protected.
    - LSST_HTTP_PUT_SEND_EXPECT: if set, a "Expect: 100-Continue" header will
        be added to all HTTP PUT requests.
        This header is required by some servers to detect if client knows how
        to handle redirections. In case of redirection, the body of the PUT
        request is sent to the redirected location.
    """
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])

    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))

    log.debug("Creating new HTTP session...")

    # Should we use a specific CA cert bundle for authenticating the server?
    if ca_bundle := os.getenv("LSST_HTTP_CACERT_BUNDLE"):
        session.verify = ca_bundle
    else:
        log.debug(
            "Environment variable LSST_HTTP_CACERT_BUNDLE is not set: "
            "if you would need to verify the remote server's certificate using "
            "issued by specific certificate authorities please consider "
            "initializing this variable."
        )

    # Should we use bearer tokens for client authentication?
    if token_path := os.getenv("LSST_HTTP_AUTH_BEARER_TOKEN"):
        # Use bearer tokens
        log.debug("... using bearer token authentication.")
        refreshToken(token_path, session)
        return session

    # Should we instead use certificate and private key? If so, both
    # LSST_HTTP_AUTH_CLIENT_CERT and LSST_HTTP_AUTH_CLIENT_KEY
    # must be initialized
    client_cert = os.getenv("LSST_HTTP_AUTH_CLIENT_CERT")
    client_key = os.getenv("LSST_HTTP_AUTH_CLIENT_KEY")
    if client_cert and client_key:
        # Use client cert and private key authentication
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
        "Neither LSST_HTTP_AUTH_BEARER_TOKEN nor (LSST_HTTP_AUTH_CLIENT_CERT and LSST_HTTP_AUTH_CLIENT_KEY) are initialized. No client authentication enabled."
    )
    return session


def sendExpectHeader() -> bool:
    """Return true if HTTP PUT requests should include the
    "Expect: 100-continue" header.

    Returns
    -------
    sendExpectHeader : `bool`
        True if LSST_HTTP_PUT_SEND_EXPECT is set, False otherwise.
    """
    # The 'Expect: 100-continue' header is used by some servers (e.g. dCache)
    # as an indication that the client knows how to handle redirects to
    # the specific server that will receive the data, in case of PUT requests.
    if "LSST_HTTP_PUT_SEND_EXPECT" in os.environ:
        log.debug("Expect: 100-Continue header enabled.")
        return True
    return False


def isTokenAuth() -> bool:
    """Return the status of bearer-token authentication.

    Returns
    -------
    isTokenAuth : `bool`
        True if LSST_BUTLER_WEBDAV_AUTH is set to TOKEN, False otherwise.
    """
    try:
        env_auth_method = os.environ["LSST_BUTLER_WEBDAV_AUTH"]
    except KeyError:
        raise KeyError(
            "Environment variable LSST_BUTLER_WEBDAV_AUTH is not set, please use values X509 or TOKEN"
        )

    if env_auth_method == "TOKEN":
        return True
    return False


def refreshToken(token_path: str, session: requests.Session) -> None:
    """Refresh the session's bearer token.

    Set or update the 'Authorization' header of the session, with the value
    fetched from file at `token_path`.

    Parameters
    ----------
    token_path : `str`
        Path to the (protected) file which contains the authentication token
    session : `requests.Session`
        Session on which bearer token authentication must be configured.
    """
    try:
        with open(token_path, "r") as fh:
            bearer_token = fh.read().replace("\n", "")
        session.headers.update({"Authorization": "Bearer " + bearer_token})
    except FileNotFoundError:
        raise FileNotFoundError(f"No authentication token file found at path: {token_path}")


@functools.lru_cache
def isWebdavEndpoint(path: Union[ResourcePath, str]) -> bool:
    """Check whether the remote HTTP endpoint implements Webdav features.

    Parameters
    ----------
    path : `ResourcePath` or `str`
        URL to the resource to be checked.
        Should preferably refer to the root since the status is shared
        by all paths in that server.

    Returns
    -------
    isWebdav : `bool`
        True if the endpoint implements Webdav, False if it doesn't.
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


def finalurl(r: requests.Response) -> str:
    """Calculate the final URL, including redirects.

    Check whether the remote HTTP endpoint redirects to a different
    endpoint, and return the final destination of the request.
    This is needed when using PUT operations, to avoid starting
    to send the data to the endpoint, before having to send it again once
    the 307 redirect response is received, and thus wasting bandwidth.

    Parameters
    ----------
    r : `requests.Response`
        An HTTP response received when requesting the endpoint

    Returns
    -------
    destination_url: `string`
        The final destination to which requests must be sent.
    """
    destination_url = r.url
    if r.status_code == 307:
        destination_url = r.headers["Location"]
        log.debug("Request redirected to %s", destination_url)
    return destination_url


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


class HttpResourcePath(ResourcePath):
    """General HTTP(S) resource."""

    _session = requests.Session()
    _sessionInitialized = False
    _is_webdav: Optional[bool] = None

    @property
    def session(self) -> requests.Session:
        """Client object to address remote resource."""
        cls = type(self)
        if cls._sessionInitialized:
            # Refresh bearer token if needed
            if token_path := os.getenv("LSST_HTTP_AUTH_BEARER_TOKEN"):
                refreshToken(token_path, cls._session)
            return cls._session

        s = getHttpSession()
        cls._session = s
        cls._sessionInitialized = True
        return s

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
        r = self.session.head(self.geturl(), timeout=TIMEOUT)

        return True if r.status_code == 200 else False

    def size(self) -> int:
        """Return the size of the remote resource in bytes."""
        if self.dirLike:
            return 0
        r = self.session.head(self.geturl(), timeout=TIMEOUT)
        if r.status_code == 200:
            return int(r.headers["Content-Length"])
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
            r = self.session.request("MKCOL", self.geturl(), timeout=TIMEOUT)
            if r.status_code != 201:
                if r.status_code == 405:
                    log.debug("Can not create directory: %s may already exist: skipping.", self.geturl())
                else:
                    raise ValueError(f"Can not create directory {self}, status code: {r.status_code}")

    def remove(self) -> None:
        """Remove the resource."""
        log.debug("Removing resource: %s", self.geturl())
        r = self.session.delete(self.geturl(), timeout=TIMEOUT)
        if r.status_code not in [200, 202, 204]:
            raise FileNotFoundError(f"Unable to delete resource {self}; status code: {r.status_code}")

    def _as_local(self) -> Tuple[str, bool]:
        """Download object over HTTP and place in temporary directory.

        Returns
        -------
        path : `str`
            Path to local temporary file.
        temporary : `bool`
            Always returns `True`. This is always a temporary file.
        """
        r = self.session.get(self.geturl(), stream=True, timeout=TIMEOUT)
        if r.status_code != 200:
            raise FileNotFoundError(f"Unable to download resource {self}; status code: {r.status_code}")

        tmpdir, buffering = _get_temp_dir()
        with tempfile.NamedTemporaryFile(
            suffix=self.getExtension(), buffering=buffering, dir=tmpdir, delete=False
        ) as tmpFile:
            with time_this(
                log,
                msg="Downloading %s [length=%s] to local file %s [chunk_size=%d]",
                args=(self, r.headers.get("Content-Length"), tmpFile.name, buffering),
            ):
                for chunk in r.iter_content(chunk_size=buffering):
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
            r = self.session.get(self.geturl(), stream=stream, timeout=TIMEOUT)
        if r.status_code != 200:
            raise FileNotFoundError(f"Unable to read resource {self}; status code: {r.status_code}")
        if not stream:
            return r.content
        else:
            return next(r.iter_content(chunk_size=size))

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
        dest_url = finalurl(self._emptyPut())
        with time_this(log, msg="Write data to remote %s", args=(self,)):
            r = self.session.put(dest_url, data=data, timeout=TIMEOUT)
        if r.status_code not in [201, 202, 204]:
            raise ValueError(f"Can not write file {self}, status code: {r.status_code}")

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
                if transfer == "move":
                    r = self.session.request(
                        "MOVE", src.geturl(), headers={"Destination": self.geturl()}, timeout=TIMEOUT
                    )
                    log.debug("Running move via MOVE HTTP request.")
                else:
                    r = self.session.request(
                        "COPY", src.geturl(), headers={"Destination": self.geturl()}, timeout=TIMEOUT
                    )
                    log.debug("Running copy via COPY HTTP request.")
        else:
            # Use local file and upload it
            with src.as_local() as local_uri:
                with open(local_uri.ospath, "rb") as f:
                    dest_url = finalurl(self._emptyPut())
                    with time_this(log, msg="Transfer from %s to %s via local file", args=(src, self)):
                        r = self.session.put(dest_url, data=f, timeout=TIMEOUT)

        if r.status_code not in [201, 202, 204]:
            raise ValueError(f"Can not transfer file {self}, status code: {r.status_code}")

        # This was an explicit move requested from a remote resource
        # try to remove that resource
        if transfer == "move":
            # Transactions do not work here
            src.remove()

    def _emptyPut(self) -> requests.Response:
        """Send an empty PUT request to current URL.

        This is used to detect if the server redirects the request to another
        endpoint, before sending actual data.

        Returns
        -------
        response : `requests.Response`
            HTTP Response from the endpoint.
        """
        headers = {"Content-Length": "0"}
        if sendExpectHeader():
            headers["Expect"] = "100-continue"
        return self.session.put(
            self.geturl(), data=None, headers=headers, allow_redirects=False, timeout=TIMEOUT
        )
