"""S3 helpers used by every pipeline stage. Kept dependency-light
(pyarrow + boto3 only) so the worker image stays reasonable to build."""

import io
import json
import os

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import Settings

_BOTO_CONFIG = Config(
    connect_timeout=10,
    read_timeout=60,
    retries={"max_attempts": 3, "mode": "standard"},
)


def get_s3_client(settings: Settings):
    """Build an S3 client from explicit credentials."""
    os.environ.pop("AWS_PROFILE", None)
    session = boto3.Session(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID") or None,
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY") or None,
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN") or None,
        region_name=settings.s3_region,
    )
    return session.client("s3", config=_BOTO_CONFIG)


def write_parquet(settings: Settings, table: pa.Table, key: str) -> str:
    """Write an Arrow table to S3 as Parquet under s3://bucket/key."""
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    client = get_s3_client(settings)
    client.upload_fileobj(buf, settings.s3_bucket, key)
    return f"s3://{settings.s3_bucket}/{key}"


def read_parquet(settings: Settings, key: str) -> pa.Table:
    client = get_s3_client(settings)
    buf = io.BytesIO()
    client.download_fileobj(settings.s3_bucket, key, buf)
    buf.seek(0)
    return pq.read_table(buf)


def object_exists(settings: Settings, key: str) -> bool:
    """True if the object exists. Uses HEAD, so it does not download anything."""
    client = get_s3_client(settings)
    try:
        client.head_object(Bucket=settings.s3_bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
            return False
        raise


def write_json(settings: Settings, obj: dict, key: str) -> str:
    """Write a small JSON document (used for cache manifests)."""
    body = json.dumps(obj, sort_keys=True).encode()
    client = get_s3_client(settings)
    client.put_object(Bucket=settings.s3_bucket, Key=key, Body=body)
    return f"s3://{settings.s3_bucket}/{key}"


def read_json(settings: Settings, key: str) -> dict | None:
    """Read a small JSON document, or None if it is absent/unreadable."""
    client = get_s3_client(settings)
    try:
        resp = client.get_object(Bucket=settings.s3_bucket, Key=key)
        return json.loads(resp["Body"].read())
    except (ClientError, ValueError):
        return None


def list_keys(settings: Settings, prefix: str) -> list[str]:
    client = get_s3_client(settings)
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def delete_keys(settings: Settings, keys: list[str]) -> int:
    """Deletes the given keys. Returns how many were deleted.

    Keys are passed explicitly (never a prefix) so a caller cannot accidentally
    wipe more than it enumerated and checked.
    """
    if not keys:
        return 0
    client = get_s3_client(settings)
    deleted = 0
    # delete_objects takes at most 1000 keys per call
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        resp = client.delete_objects(
            Bucket=settings.s3_bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        errors = resp.get("Errors", [])
        if errors:
            raise RuntimeError(f"Failed to delete {len(errors)} object(s): {errors[:3]}")
        deleted += len(batch)
    return deleted
