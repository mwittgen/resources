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

import unittest

from lsst.resources import ResourcePath
from lsst.resources.tests import GenericReadWriteTestCase, GenericTestCase

try:
    import boto3
    import botocore
    from moto import mock_s3
except ImportError:
    boto3 = None

    def mock_s3(cls):
        """A no-op decorator in case moto mock_s3 can not be imported."""
        return cls


class GenericFileTestCase(GenericTestCase, unittest.TestCase):
    scheme = "s3"
    netloc = "my_bucket"


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
class FileReadWriteTestCase(GenericReadWriteTestCase, unittest.TestCase):
    scheme = "s3"
    netloc = "my_2nd_bucket"

    mock_s3 = mock_s3()
    """The mocked s3 interface from moto."""

    def setUp(self):
        # Enable S3 mocking of tests.
        self.mock_s3.start()

        # set up some fake credentials if they do not exist
        # self.usingDummyCredentials = setAwsEnvCredentials()

        # MOTO needs to know that we expect Bucket bucketname to exist
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.netloc)

        super().setUp()

    def tearDown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.netloc)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the key was not reachable - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.netloc)
        bucket.delete()

        # Stop the S3 mock.
        self.mock_s3.stop()

        super().tearDown()

    def test_bucket_fail(self):
        # Deliberately create URI with unknown bucket.
        uri = ResourcePath("s3://badbucket/something/")

        with self.assertRaises(ValueError):
            uri.mkdir()

        with self.assertRaises(FileNotFoundError):
            uri.remove()

    def test_transfer_progress(self):
        """Test progress bar reporting for upload and download."""
        remote = self.root_uri.join("test.dat")
        remote.write(b"42")
        with ResourcePath.temporary_uri(suffix=".dat") as tmp:
            # Download from S3.
            with self.assertLogs("lsst.resources", level="DEBUG") as cm:
                tmp.transfer_from(remote, transfer="auto")
            self.assertRegex("".join(cm.output), r"test\.dat.*100\%")

            # Upload to S3.
            with self.assertLogs("lsst.resources", level="DEBUG") as cm:
                remote.transfer_from(tmp, transfer="auto", overwrite=True)
            self.assertRegex("".join(cm.output), rf"{tmp.basename()}.*100\%")


if __name__ == "__main__":
    unittest.main()
