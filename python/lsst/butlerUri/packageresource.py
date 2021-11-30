# This file is part of butlerUri.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# Use of this source code is governed by a 3-clause BSD-style
# license that can be found in the LICENSE file.

import pkg_resources
import logging

__all__ = ('ButlerPackageResourceURI',)

from ._butlerUri import ButlerURI

log = logging.getLogger(__name__)


class ButlerPackageResourceURI(ButlerURI):
    """URI referring to a Python package resource.

    These URIs look like: ``resource://lsst.daf.butler/configs/file.yaml``
    where the network location is the Python package and the path is the
    resource name.
    """

    def exists(self) -> bool:
        """Check that the python resource exists."""
        return pkg_resources.resource_exists(self.netloc, self.relativeToPathRoot)

    def read(self, size: int = -1) -> bytes:
        """Read the contents of the resource."""
        with pkg_resources.resource_stream(self.netloc, self.relativeToPathRoot) as fh:
            return fh.read(size)
