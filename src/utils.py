import json
import logging
from contextlib import contextmanager
from typing import Iterator

from dagster_pipes import PipesMessage, PipesMessageWriter, PipesMessageWriterChannel, PipesParams


class PipesLoggerMessageWriterChannel(PipesMessageWriterChannel):

    def __init__(self):
        self._logger = logging.getLogger("pipesLogger")

    def write_message(self, message: PipesMessage):
        self._logger.info(json.dumps(message))


class PipesLoggerMessageWriter(PipesMessageWriter):

    @contextmanager
    def open(self, params: PipesParams) -> Iterator[PipesLoggerMessageWriterChannel]:
        yield PipesLoggerMessageWriterChannel()
