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

import importlib
import os.path
import stat
import tempfile
import unittest

import lsst.resources
import requests
import responses
from lsst.resources import ResourcePath
from lsst.resources.http import BearerTokenAuth, _get_http_session, _send_expect_header_on_put
from lsst.resources.tests import GenericTestCase
from lsst.resources.utils import makeTestTempDir, removeTestTempDir

TESTDIR = os.path.abspath(os.path.dirname(__file__))


class GenericHttpTestCase(GenericTestCase, unittest.TestCase):
    scheme = "http"
    netloc = "server.example"


class HttpReadWriteTestCase(unittest.TestCase):
    """Specialist test cases for WebDAV server.

    The responses class requires that every possible request be explicitly
    mocked out.  This currently makes it extremely inconvenient to subclass
    the generic read/write tests shared by other URI schemes.  For now use
    explicit standalone tests.
    """

    def setUp(self):
        # Local test directory
        self.tmpdir = ResourcePath(makeTestTempDir(TESTDIR))

        serverRoot = "www.not-exists.orgx"
        existingFolderName = "existingFolder"
        existingFileName = "existingFile"
        notExistingFileName = "notExistingFile"

        self.baseURL = ResourcePath(f"https://{serverRoot}", forceDirectory=True)
        self.existingFileResourcePath = ResourcePath(
            f"https://{serverRoot}/{existingFolderName}/{existingFileName}"
        )
        self.notExistingFileResourcePath = ResourcePath(
            f"https://{serverRoot}/{existingFolderName}/{notExistingFileName}"
        )
        self.existingFolderResourcePath = ResourcePath(
            f"https://{serverRoot}/{existingFolderName}", forceDirectory=True
        )
        self.notExistingFolderResourcePath = ResourcePath(
            f"https://{serverRoot}/{notExistingFileName}", forceDirectory=True
        )

        # Need to declare the options
        responses.add(responses.OPTIONS, self.baseURL.geturl(), status=200, headers={"DAV": "1,2,3"})

        # Used by HttpResourcePath.exists()
        responses.add(
            responses.HEAD,
            self.existingFileResourcePath.geturl(),
            status=200,
            headers={"Content-Length": "1024"},
        )
        responses.add(responses.HEAD, self.notExistingFileResourcePath.geturl(), status=404)

        # Used by HttpResourcePath.read()
        responses.add(
            responses.GET, self.existingFileResourcePath.geturl(), status=200, body=str.encode("It works!")
        )
        responses.add(responses.GET, self.notExistingFileResourcePath.geturl(), status=404)

        # Used by HttpResourcePath.write()
        responses.add(responses.PUT, self.existingFileResourcePath.geturl(), status=201)

        # Used by HttpResourcePath.transfer_from()
        responses.add(
            responses.Response(
                url=self.existingFileResourcePath.geturl(),
                method="COPY",
                headers={"Destination": self.existingFileResourcePath.geturl()},
                status=201,
            )
        )
        responses.add(
            responses.Response(
                url=self.existingFileResourcePath.geturl(),
                method="COPY",
                headers={"Destination": self.notExistingFileResourcePath.geturl()},
                status=201,
            )
        )
        responses.add(
            responses.Response(
                url=self.existingFileResourcePath.geturl(),
                method="MOVE",
                headers={"Destination": self.notExistingFileResourcePath.geturl()},
                status=201,
            )
        )

        # Used by HttpResourcePath.remove()
        responses.add(responses.DELETE, self.existingFileResourcePath.geturl(), status=200)
        responses.add(responses.DELETE, self.notExistingFileResourcePath.geturl(), status=404)

        # Used by HttpResourcePath.mkdir()
        responses.add(
            responses.HEAD,
            self.existingFolderResourcePath.geturl(),
            status=200,
            headers={"Content-Length": "1024"},
        )
        responses.add(responses.HEAD, self.baseURL.geturl(), status=200, headers={"Content-Length": "1024"})
        responses.add(responses.HEAD, self.notExistingFolderResourcePath.geturl(), status=404)
        responses.add(
            responses.Response(url=self.notExistingFolderResourcePath.geturl(), method="MKCOL", status=201)
        )
        responses.add(
            responses.Response(url=self.existingFolderResourcePath.geturl(), method="MKCOL", status=403)
        )

    def tearDown(self):
        if self.tmpdir:
            if self.tmpdir.isLocal:
                removeTestTempDir(self.tmpdir.ospath)

    @responses.activate
    def test_exists(self):

        self.assertTrue(self.existingFileResourcePath.exists())
        self.assertFalse(self.notExistingFileResourcePath.exists())

        self.assertEqual(self.existingFileResourcePath.size(), 1024)
        with self.assertRaises(FileNotFoundError):
            self.notExistingFileResourcePath.size()

    @responses.activate
    def test_remove(self):

        self.assertIsNone(self.existingFileResourcePath.remove())
        with self.assertRaises(FileNotFoundError):
            self.notExistingFileResourcePath.remove()

        with self.assertRaises(FileNotFoundError):
            url = "https://example.org/delete"
            responses.add(responses.DELETE, url, status=404)
            ResourcePath(url).remove()

    @responses.activate
    def test_mkdir(self):

        # The mock means that we can't check this now exists
        self.notExistingFolderResourcePath.mkdir()

        # This should do nothing
        self.existingFolderResourcePath.mkdir()

        with self.assertRaises(ValueError):
            self.notExistingFileResourcePath.mkdir()

    @responses.activate
    def test_read(self):

        self.assertEqual(self.existingFileResourcePath.read().decode(), "It works!")
        self.assertNotEqual(self.existingFileResourcePath.read().decode(), "Nope.")
        with self.assertRaises(FileNotFoundError):
            self.notExistingFileResourcePath.read()

        # Run this twice to ensure use of cache in code coverag.
        for _ in (1, 2):
            with self.existingFileResourcePath.as_local() as local_uri:
                self.assertTrue(local_uri.isLocal)
                content = local_uri.read().decode()
                self.assertEqual(content, "It works!")

        # Check that the environment variable is being read.
        lsst.resources.http._TMPDIR = None
        with unittest.mock.patch.dict(os.environ, {"LSST_RESOURCES_TMPDIR": self.tmpdir.ospath}):
            with self.existingFileResourcePath.as_local() as local_uri:
                self.assertTrue(local_uri.isLocal)
                content = local_uri.read().decode()
                self.assertEqual(content, "It works!")
                self.assertIsNotNone(local_uri.relative_to(self.tmpdir))

    @responses.activate
    def test_write(self):

        self.assertIsNone(self.existingFileResourcePath.write(data=str.encode("Some content.")))
        with self.assertRaises(FileExistsError):
            self.existingFileResourcePath.write(data=str.encode("Some content."), overwrite=False)

        with self.assertRaises(ValueError):
            url = "https://example.org/put"
            responses.add(responses.PUT, url, status=404)
            ResourcePath(url).write(data=str.encode("Some content."))

    @responses.activate
    def test_transfer(self):

        # Transferring to self should be no-op.
        self.existingFileResourcePath.transfer_from(src=self.existingFileResourcePath)

        self.assertIsNone(self.notExistingFileResourcePath.transfer_from(src=self.existingFileResourcePath))
        # Should test for existence.
        # self.assertTrue(self.notExistingFileResourcePath.exists())

        # Should delete and try again with move.
        # self.notExistingFileResourcePath.remove()
        self.assertIsNone(
            self.notExistingFileResourcePath.transfer_from(src=self.existingFileResourcePath, transfer="move")
        )
        # Should then check that it was moved.
        # self.assertFalse(self.existingFileResourcePath.exists())

        # Existing file resource should have been removed so this should
        # trigger FileNotFoundError.
        # with self.assertRaises(FileNotFoundError):
        #    self.notExistingFileResourcePath.transfer_from(src=self.existingFileResourcePath)
        with self.assertRaises(ValueError):
            self.notExistingFileResourcePath.transfer_from(
                src=self.existingFileResourcePath, transfer="unsupported"
            )

    def test_parent(self):

        self.assertEqual(
            self.existingFolderResourcePath.geturl(), self.notExistingFileResourcePath.parent().geturl()
        )
        self.assertEqual(self.baseURL.geturl(), self.baseURL.parent().geturl())
        self.assertEqual(
            self.existingFileResourcePath.parent().geturl(), self.existingFileResourcePath.dirname().geturl()
        )

    def test_ca_cert_bundle(self):
        with tempfile.NamedTemporaryFile(mode="wt", dir=self.tmpdir.ospath, delete=False) as f:
            f.write("CERT BUNDLE")
            cert_bundle = f.name

        with unittest.mock.patch.dict(os.environ, {"LSST_HTTP_CACERT_BUNDLE": cert_bundle}, clear=True):
            session = _get_http_session(self.baseURL)
            self.assertTrue(session.verify == cert_bundle)

    def test_token(self):
        # For test coverage
        auth = BearerTokenAuth(None)
        auth._refresh()
        self.assertTrue(auth._token is None and auth._path is None)
        req = requests.Request("GET", "https://example.org")
        self.assertTrue(auth(req) == req)

        # Create a mock token file
        with tempfile.NamedTemporaryFile(mode="wt", dir=self.tmpdir.ospath, delete=False) as f:
            f.write("ABCDE")
            token_path = f.name

        with unittest.mock.patch.dict(os.environ, {"LSST_HTTP_AUTH_BEARER_TOKEN": token_path}, clear=True):
            # Ensure the sessions authentication mechanism is correctly set
            # to use a bearer token when only the owner can access the bearer
            # token file
            os.chmod(token_path, stat.S_IRUSR)
            session = _get_http_session(self.baseURL)
            self.assertTrue(type(session.auth) == lsst.resources.http.BearerTokenAuth)

            # Ensure an exception is raised if either group or other can read
            # the token file
            for mode in (stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP, stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH):
                os.chmod(token_path, stat.S_IRUSR | mode)
                with self.assertRaises(PermissionError):
                    BearerTokenAuth(token_path)

    def test_send_expect_header(self):
        with unittest.mock.patch.dict(os.environ, {"LSST_HTTP_PUT_SEND_EXPECT_HEADER": "true"}, clear=True):
            _send_expect_header_on_put.cache_clear()
            self.assertTrue(_send_expect_header_on_put())

            _send_expect_header_on_put.cache_clear()
            del os.environ["LSST_HTTP_PUT_SEND_EXPECT_HEADER"]
            self.assertFalse(_send_expect_header_on_put())

    def test_user_cert(self):
        # Create mock certificate and private key files
        with tempfile.NamedTemporaryFile(mode="wt", dir=self.tmpdir.ospath, delete=False) as f:
            f.write("CERT")
            client_cert = f.name

        with tempfile.NamedTemporaryFile(mode="wt", dir=self.tmpdir.ospath, delete=False) as f:
            f.write("KEY")
            client_key = f.name

        # Check both LSST_HTTP_AUTH_CLIENT_CERT and LSST_HTTP_AUTH_CLIENT_KEY
        # must be initialized
        with unittest.mock.patch.dict(os.environ, {"LSST_HTTP_AUTH_CLIENT_CERT": client_cert}, clear=True):
            with self.assertRaises(ValueError):
                _get_http_session(self.baseURL)

        with unittest.mock.patch.dict(os.environ, {"LSST_HTTP_AUTH_CLIENT_KEY": client_key}, clear=True):
            with self.assertRaises(ValueError):
                _get_http_session(self.baseURL)

        # Check private key must be accessible only by its owner
        with unittest.mock.patch.dict(
            os.environ,
            {"LSST_HTTP_AUTH_CLIENT_CERT": client_cert, "LSST_HTTP_AUTH_CLIENT_KEY": client_key},
            clear=True,
        ):
            # Ensure the session client certificate is initialized when
            # only the owner can read the private key file
            os.chmod(client_key, stat.S_IRUSR)
            session = _get_http_session(self.baseURL)
            self.assertTrue(session.cert[0] == client_cert)
            self.assertTrue(session.cert[1] == client_key)

            # Ensure an exception is raised if either group or other can access
            # the private key file
            for mode in (stat.S_IRGRP, stat.S_IWGRP, stat.S_IXGRP, stat.S_IROTH, stat.S_IWOTH, stat.S_IXOTH):
                os.chmod(client_key, stat.S_IRUSR | mode)
                with self.assertRaises(PermissionError):
                    _get_http_session(self.baseURL)

    def test_sessions(self):
        path = ResourcePath(self.baseURL)
        self.assertTrue(self.baseURL.session is not None and self.baseURL.session == path.session)
        self.assertTrue(
            self.baseURL.upload_session is not None and self.baseURL.upload_session == path.upload_session
        )

    def test_timeout(self):
        connect_timeout = 100
        read_timeout = 200
        with unittest.mock.patch.dict(
            os.environ,
            {"LSST_HTTP_TIMEOUT_CONNECT": str(connect_timeout), "LSST_HTTP_TIMEOUT_READ": str(read_timeout)},
            clear=True,
        ):
            # Force module reload to initialize TIMEOUT
            importlib.reload(lsst.resources.http)
            self.assertTrue(lsst.resources.http.TIMEOUT == (connect_timeout, read_timeout))


if __name__ == "__main__":
    unittest.main()
