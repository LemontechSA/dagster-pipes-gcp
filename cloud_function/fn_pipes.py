import json
from contextlib import contextmanager
from typing import IO, Iterator, Optional

import google.cloud.logging
import google.cloud.storage
from dagster_pipes import (  # _assert_opt_env_param_type
    PipesBlobStoreMessageWriter,
    PipesBlobStoreMessageWriterChannel,
    PipesMessage,
    PipesMessageWriter,
    PipesMessageWriterChannel,
    PipesParams,
    _assert_env_param_type,
    _assert_opt_env_param_type,
)

logging_client = google.cloud.logging.Client()


class PipesCloudStorageMessageWriter(PipesBlobStoreMessageWriter):
    """Message writer that writes messages by periodically writing message chunks to an S3 bucket.

    Args:
        client (Any): A boto3.client("s3") object.
        interval (float): interval in seconds between upload chunk uploads
    """

    def __init__(
        self,
        client: google.cloud.storage.Client,
        *,
        interval: float = 10,
    ):
        super().__init__(interval=interval)
        self._client = client

    def make_channel(
        self,
        params: PipesParams,
    ) -> "PipesCloudStorageMessageWriterChannel":
        bucket = _assert_env_param_type(params, "bucket", str, self.__class__)
        key_prefix = _assert_opt_env_param_type(params, "key_prefix", str, self.__class__)
        return PipesCloudStorageMessageWriterChannel(
            client=self._client,
            bucket=bucket,
            key_prefix=key_prefix,
            interval=self.interval,
        )


class PipesCloudStorageMessageWriterChannel(PipesBlobStoreMessageWriterChannel):
    """Message writer channel for writing messages by periodically writing message chunks to an S3 bucket.

    Args:
        client (Any): A boto3.client("s3") object.
        bucket (str): The name of the S3 bucket to write to.
        key_prefix (Optional[str]): An optional prefix to use for the keys of written blobs.
        interval (float): interval in seconds between upload chunk uploads
    """

    def __init__(
        self,
        client: google.cloud.storage.Client,
        bucket: str,
        key_prefix: Optional[str],
        *,
        interval: float = 10,
    ):
        super().__init__(interval=interval)
        self._client = client
        self._bucket = client.bucket(bucket)
        self._key_prefix = key_prefix

    def upload_messages_chunk(self, payload: IO, index: int) -> None:
        key = f"{self._key_prefix}/{index}.json" if self._key_prefix else f"{index}.json"
        blob = self._bucket.blob(key)
        blob.upload_from_string(payload.read())


class PipesCloudLoggerMessageWriterChannel(PipesMessageWriterChannel):

    def __init__(self, trace: str):
        self._trace = trace
        self._logger = logging_client.logger("dagster-pipes-utils")

    def write_message(self, message: PipesMessage):
        self._logger.log_struct(
            {
                "severity": "INFO",
                "message": json.dumps(message),
                "trace": self._trace,
            }
        )


class PipesCloudLoggerMessageWriter(PipesMessageWriter):

    def __init__(self, trace: str) -> None:
        super().__init__()
        self._trace = trace

    @contextmanager
    def open(self, params: PipesParams) -> Iterator[PipesCloudLoggerMessageWriterChannel]:
        yield PipesCloudLoggerMessageWriterChannel(self._trace)
