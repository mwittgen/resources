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


class SchemelessTestCase(unittest.TestCase):
    """Test the behavior of a schemeless URI."""

    def test_creation(self) -> None:
        """Test creation from schemeless URI."""
        relative = "a/b/c.txt"
        abspath = "/a/b/c.txt"

        relative_uri = ResourcePath(relative, forceAbsolute=False)
        self.assertFalse(relative_uri.scheme)
        self.assertFalse(relative_uri.isabs())
        self.assertEqual(relative_uri.ospath, relative)

        # This will be a schemeless absolute URI.
        # It is not converted to a file URI (but maybe should be).
        abs_uri = ResourcePath(relative, forceAbsolute=True)
        self.assertFalse(abs_uri.scheme)
        self.assertTrue(abs_uri.isabs())

        # For historical reasons an absolute path is converted
        # to a file URI.
        file_uri = ResourcePath(abspath)
        self.assertEqual(file_uri.scheme, "file")
        self.assertTrue(file_uri.isabs())

        # Use a prefix root.
        # This will remain schemeless.
        prefix = "/a/b/"
        abs_uri = ResourcePath(relative, root=prefix)
        self.assertEqual(abs_uri.ospath, f"{prefix}{relative}")
        self.assertEqual(abs_uri.scheme, "")

        # Only the path is used.
        prefix = "file://localhost/a/b/"
        prefix_uri = ResourcePath(prefix)
        file_uri = ResourcePath(relative, root=prefix_uri)
        self.assertEqual(str(file_uri), f"{prefix_uri.ospath}{relative}")

        # Fragments should be fine.
        relative_uri = ResourcePath(relative + "#frag", forceAbsolute=False)
        self.assertEqual(str(relative_uri), f"{relative}#frag")

        # For historical reasons a a root can not be anything other
        # than a file. This does not really make sense in the general
        # sense but can be implemented using uri.join().
        with self.assertRaises(ValueError):
            ResourcePath(relative, root=ResourcePath("resource://lsst.resources/something.txt"))


if __name__ == "__main__":
    unittest.main()
