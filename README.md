# butlerUri

This package provides a simple interface to local or remote files using URIs.

```
from lsst.butlerUri import ButlerURI

file_uri = ButlerURI("/data/file.txt")
contents = file_uri.read()

s3_uri = ButlerURI("s3://bucket/data/file.txt")
contents = s3_uri.read()
```

The package currently understands `file`, `s3`, `http[s]`, and `resource` (Python package resource) URI schemes as well as a scheme-less URI (relative local file path).

The name derives from the package's origin as the core component of the [Rubin Observatory Data Butler](https://github.com/lsst/daf_butler) datastore.
