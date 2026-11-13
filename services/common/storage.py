"""Object storage: an S3-compatible store (SeaweedFS) for instrument data.

A thin wrapper so you do not have to set up a client. Every call can fail:
the store can be slow or down. Failures raise StorageError.
"""

import asyncio
import logging
from dataclasses import dataclass

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger(__name__)


class StorageError(Exception):
    """The object store could not complete a request."""


@dataclass(frozen=True)
class StoredObject:
    key: str
    size: int


class Storage:
    def __init__(self, endpoint: str, bucket: str) -> None:
        self.bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            # SeaweedFS runs here without authentication; boto3 still wants keys.
            aws_access_key_id="lab",
            aws_secret_access_key="lab",
            region_name="us-east-1",
            config=Config(
                connect_timeout=2,
                read_timeout=5,
                retries={"max_attempts": 1, "mode": "standard"},
            ),
        )

    def ref(self, key: str) -> str:
        """The reference for a key, in the form the API reports: s3://bucket/key."""
        return f"s3://{self.bucket}/{key}"

    async def ensure_bucket(self) -> None:
        try:
            await asyncio.to_thread(self._client.create_bucket, Bucket=self.bucket)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                return
            raise StorageError(f"create bucket {self.bucket}: {exc}") from exc
        except BotoCoreError as exc:
            raise StorageError(f"create bucket {self.bucket}: {exc}") from exc

    async def put(self, key: str, data: bytes) -> str:
        """Store data under key, replacing anything already there. Returns the ref."""
        try:
            await asyncio.to_thread(self._client.put_object, Bucket=self.bucket, Key=key, Body=data)
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"put {key}: {exc}") from exc
        return self.ref(key)

    async def get(self, key: str) -> bytes:
        try:
            resp = await asyncio.to_thread(self._client.get_object, Bucket=self.bucket, Key=key)
            body: bytes = await asyncio.to_thread(resp["Body"].read)
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"get {key}: {exc}") from exc
        return body

    async def exists(self, key: str) -> bool:
        try:
            await asyncio.to_thread(self._client.head_object, Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code", "") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise StorageError(f"head {key}: {exc}") from exc
        except BotoCoreError as exc:
            raise StorageError(f"head {key}: {exc}") from exc
        return True

    async def delete(self, key: str) -> None:
        try:
            await asyncio.to_thread(self._client.delete_object, Bucket=self.bucket, Key=key)
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"delete {key}: {exc}") from exc

    async def list(self, prefix: str = "") -> list[StoredObject]:
        out: list[StoredObject] = []
        token: str | None = None
        try:
            while True:
                if token:
                    resp = await asyncio.to_thread(
                        self._client.list_objects_v2,
                        Bucket=self.bucket,
                        Prefix=prefix,
                        ContinuationToken=token,
                    )
                else:
                    resp = await asyncio.to_thread(
                        self._client.list_objects_v2, Bucket=self.bucket, Prefix=prefix
                    )
                for obj in resp.get("Contents", []):
                    out.append(StoredObject(key=obj["Key"], size=int(obj["Size"])))
                if not resp.get("IsTruncated"):
                    return out
                token = resp.get("NextContinuationToken")
        except (BotoCoreError, ClientError) as exc:
            raise StorageError(f"list {prefix!r}: {exc}") from exc
