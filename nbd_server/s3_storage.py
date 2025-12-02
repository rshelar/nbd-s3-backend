from typing import Optional

import boto3
from botocore.exceptions import ClientError

from nbd_server.storage import Storage
from nbd_server.util import BLOCK_SIZE


class S3Storage(Storage):
    """
    S3-backed block storage.

    Blocks are stored under:
        exports/<export_name>/blocks/<block_id>

    Missing blocks return zero-filled bytes(BLOCK_SIZE).
    """

    def __init__(
            self,
            bucket: str,
            export_name: str,
            endpoint_url: Optional[str] = None,
            region: str = "us-east-1",
            aws_access_key_id: Optional[str] = None,
            aws_secret_access_key: Optional[str] = None,
    ) -> None:
        """
        Args:
            bucket: S3 bucket name.
            export_name: logical export (device name) namespace.
            endpoint_url: Optional MinIO URL (e.g., http://localhost:9000)
            region: AWS region (ignored for MinIO).
            aws_access_key_id / aws_secret_access_key: credentials.
        """
        self.bucket = bucket
        self.export_name = export_name

        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    # ------------------------
    # Internal helpers
    # ------------------------

    def _key(self, block_id: int) -> str:
        """
        S3 key for this block.
        """
        return f"exports/{self.export_name}/blocks/{block_id}"

    def _key_tmp(self, block_id: int) -> str:
        """
        Temporary key used for atomic write.
        """
        return f"exports/{self.export_name}/blocks/{block_id}.tmp"

    # ------------------------
    # Storage API
    # ------------------------

    def read_block(self, export_name: str, block_id: int) -> bytes:
        """
        Read a block from S3. Missing object → return zero-filled block.
        """
        key = self._key(block_id)

        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=key)
            data = resp["Body"].read()
        except ClientError as e:
            err = e.response["Error"]["Code"]
            if err in ("NoSuchKey", "404"):
                return bytes(BLOCK_SIZE)  # zero-fill
            raise

        # Normalize to BLOCK_SIZE
        if len(data) < BLOCK_SIZE:
            data = data + bytes(BLOCK_SIZE - len(data))
        elif len(data) > BLOCK_SIZE:
            data = data[:BLOCK_SIZE]

        return data

    def write_block(self, export_name: str, block_id: int, data: bytes) -> None:
        """
        Write a full block to S3 atomically.
        Upload to a temporary key, then overwrite the final key.
        """
        if len(data) != BLOCK_SIZE:
            raise ValueError(
                f"data must be exactly {BLOCK_SIZE} bytes; got {len(data)} bytes"
            )

        key = self._key(block_id)
        tmp_key = self._key_tmp(block_id)

        # Upload to temporary object
        self.s3.put_object(
            Bucket=self.bucket,
            Key=tmp_key,
            Body=data,
        )

        # Copy temp → real key (atomic S3-side copy)
        self.s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": tmp_key},
            Key=key,
        )

        # Remove the temporary object
        self.s3.delete_object(Bucket=self.bucket, Key=tmp_key)