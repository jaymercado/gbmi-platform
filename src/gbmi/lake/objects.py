"""Whole objects of the lake beside the Iceberg tables: raw files and quarantine files.

One write is atomic on both backends (blueprint 5.6): a reader sees the whole object or none.

* **Local file system.** The bytes go to ``<name>.part``, and the file is moved to its name.
* **S3.** One PUT request for the final key. S3 shows an object only when the request has
  ended, so the write needs no temporary object, no move and no delete (week 3 plan 3.3).
  PyArrow would write an empty marker object for the "directory", copy and delete for a move,
  and send three requests for one multipart upload.

The role of the AWS host therefore needs no right to delete below ``raw/``.

A removal goes the same way: one DELETE request on S3. PyArrow would write an empty marker
object for the "directory" of the removed file (found by the AWS half of the integration
tests on 2026-11-28).
"""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
from typing import Any
from urllib.parse import urlsplit

import pyarrow.fs as pafs

S3_SCHEME = "s3"


@lru_cache(maxsize=1)
def s3_client() -> Any:
    """The S3 client of the process.

    The credentials come from the environment: the role of the host, or short-lived keys on a
    laptop. Nothing long-lived is configured here (blueprint 5.9).
    """
    # Imported here so that the local backend needs no boto3 at import time.
    import boto3

    return boto3.client("s3")


def is_s3(lake_uri: str) -> bool:
    return urlsplit(lake_uri).scheme == S3_SCHEME


def s3_location(lake_uri: str, key: str) -> tuple[str, str]:
    """The bucket and the object key of a key below the lake root."""
    parts = urlsplit(lake_uri)
    prefix = parts.path.strip("/")
    return parts.netloc, f"{prefix}/{key}" if prefix else key


def write_object(
    lake_uri: str,
    key: str,
    content: bytes,
    *,
    before_visible: Callable[[], None] | None = None,
    client: Any | None = None,
) -> None:
    """Write one object below the lake root.

    ``before_visible`` runs at the last moment at which no reader can see the object: a drill
    stops the process there. ``client`` replaces the S3 client of the process in a test.
    """
    if is_s3(lake_uri):
        bucket, object_key = s3_location(lake_uri, key)
        if before_visible is not None:
            before_visible()
        (client or s3_client()).put_object(Bucket=bucket, Key=object_key, Body=content)
        return
    filesystem, root = pafs.FileSystem.from_uri(lake_uri)
    path = f"{root.rstrip('/')}/{key}"
    filesystem.create_dir(path.rsplit("/", 1)[0], recursive=True)
    temporary = f"{path}.part"
    with filesystem.open_output_stream(temporary) as stream:
        stream.write(content)
    if before_visible is not None:
        before_visible()
    filesystem.move(temporary, path)


def delete_object(lake_uri: str, key: str, *, client: Any | None = None) -> bool:
    """Remove one object below the lake root; returns False when no such object existed.

    On S3: one HEAD request, which says whether the object exists, and one DELETE request for
    the key. Nothing else is written. ``client`` replaces the S3 client of the process in a
    test.
    """
    if is_s3(lake_uri):
        bucket, object_key = s3_location(lake_uri, key)
        s3 = client or s3_client()
        try:
            s3.head_object(Bucket=bucket, Key=object_key)
        except s3.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
        s3.delete_object(Bucket=bucket, Key=object_key)
        return True
    filesystem, root = pafs.FileSystem.from_uri(lake_uri)
    path = f"{root.rstrip('/')}/{key}"
    if filesystem.get_file_info(path).type == pafs.FileType.NotFound:
        return False
    filesystem.delete_file(path)
    return True
