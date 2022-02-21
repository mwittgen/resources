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

import os.path
import stat
import tempfile
import unittest

import lsst.resources
import responses
from lsst.resources import ResourcePath
from lsst.resources.tests import GenericTestCase
from lsst.resources.utils import makeTestTempDir, removeTestTempDir
from lsst.resources.http import BearerTokenAuth

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

    def test_token(self):
        # Create a mock token file
        with tempfile.NamedTemporaryFile(mode="wt", dir=TESTDIR, delete=False) as f:
            f.write("ABCDE")
            token_path = f.name

        with unittest.mock.patch.dict(os.environ, {"LSST_HTTP_AUTH_BEARER_TOKEN": token_path}):
            # Ensure the owner can read the token file
            os.chmod(token_path, stat.S_IRUSR)
            BearerTokenAuth(token_path)

            # Ensure an exception is raised if either group or other can read
            # the token file
            for mode in (stat.S_IRGRP, stat.S_IWGRP, stat.S_IROTH, stat.S_IWOTH):
                os.chmod(token_path, stat.S_IRUSR | mode)
                with self.assertRaises(PermissionError):
                    BearerTokenAuth(token_path)


if __name__ == "__main__":
    unittest.main()
