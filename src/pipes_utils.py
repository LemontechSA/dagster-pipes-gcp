import json
import logging
from contextlib import contextmanager
from typing import Iterator

# import google.cloud.logging
from dagster_pipes import PipesMessage, PipesMessageWriter, PipesMessageWriterChannel, PipesParams

# client = google.cloud.logging.Client()


class PipesLoggerMessageWriterChannel(PipesMessageWriterChannel):

    def __init__(self, trace: str):
        self._trace = trace
        # self._logger = client.logger("dagster-pipes-utils")
        self._logger = logging.getLogger("dagster-pipes-utils")

    def write_message(self, message: PipesMessage):
        self._logger.info(json.dumps(message), extra={"trace": self._trace})
        # self._logger.log_struct(
        #     {
        #         "severity": "INFO",
        #         "message": json.dumps(message),
        #         "trace": self._trace,
        #     }
        # )


class PipesLoggerMessageWriter(PipesMessageWriter):

    def __init__(self, trace: str) -> None:
        super().__init__()
        self._trace = trace

    @contextmanager
    def open(self, params: PipesParams) -> Iterator[PipesLoggerMessageWriterChannel]:
        yield PipesLoggerMessageWriterChannel(self._trace)
