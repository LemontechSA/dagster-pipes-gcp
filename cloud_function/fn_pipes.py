import json
from contextlib import contextmanager
from typing import Iterator

import google.cloud.logging
from dagster_pipes import PipesMessage, PipesMessageWriter, PipesMessageWriterChannel, PipesParams

client = google.cloud.logging.Client()


class PipesCloudLoggerMessageWriterChannel(PipesMessageWriterChannel):

    def __init__(self, trace: str):
        self._trace = trace
        self._logger = client.logger("dagster-pipes-utils")

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
