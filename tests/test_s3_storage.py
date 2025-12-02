import boto3
import pytest

from botocore.exceptions import ClientError

from nbd_server.s3_storage import S3Storage
from nbd_server.util import BLOCK_SIZE


BUCKET = "nbdbucket"
EXPORT = "dev1"


@pytest.fixture(scope="module")
def s3_client():
    """Return a low-level boto3 client connected to MinIO."""
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )


@pytest.fixture(scope="module", autouse=True)
def ensure_bucket(s3_client):
    """Ensure the test bucket exists."""
    # Try to create bucket
    try:
        s3_client.create_bucket(Bucket=BUCKET)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise


@pytest.fixture
def storage():
    """Return an S3Storage backend."""
    return S3Storage(
        bucket=BUCKET,
        export_name=EXPORT,
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )


def test_read_missing_block_returns_zero_fill(storage):
    data = storage.read_block(EXPORT, 999)  # arbitrary block id
    assert data == bytes(BLOCK_SIZE)
    assert len(data) == BLOCK_SIZE


def test_write_and_read_roundtrip(storage):
    block_id = 5
    write_data = b"hello" + bytes(BLOCK_SIZE - 5)

    storage.write_block(EXPORT, block_id, write_data)
    out = storage.read_block(EXPORT, block_id)

    assert out == write_data
    assert len(out) == BLOCK_SIZE


def test_atomic_write(storage, s3_client):
    block_id = 7
    tmp_key = f"exports/{EXPORT}/blocks/{block_id}.tmp"
    real_key = f"exports/{EXPORT}/blocks/{block_id}"

    write_data = b"A" + bytes(BLOCK_SIZE - 1)
    storage.write_block(EXPORT, block_id, write_data)

    # temp file must NOT exist
    with pytest.raises(ClientError):
        s3_client.get_object(Bucket=BUCKET, Key=tmp_key)

    # real key must exist
    resp = s3_client.get_object(Bucket=BUCKET, Key=real_key)
    body = resp["Body"].read()
    assert body == write_data


def test_normalizes_short_blocks(storage, s3_client):
    block_id = 9
    key = f"exports/{EXPORT}/blocks/{block_id}"

    # Upload a short object manually to S3
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=b"XYZ")

    out = storage.read_block(EXPORT, block_id)
    assert out.startswith(b"XYZ")
    assert out[3:] == bytes(BLOCK_SIZE - 3)
    assert len(out) == BLOCK_SIZE