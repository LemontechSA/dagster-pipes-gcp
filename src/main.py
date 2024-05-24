import logging
import uuid

import flask
import google.cloud.logging
from dagster_pipes import PipesMappingParamsLoader, open_dagster_pipes
from google.cloud.logging_v2.handlers import setup_logging

from pipes_utils import PipesLoggerMessageWriter
from version import __version__

EXECUTION_ID = str(uuid.uuid4())


class ContextFilter(logging.Filter):
    def filter(self, record):
        record.json_fields = {"execution_id": EXECUTION_ID}
        return True


client = google.cloud.logging.Client()
handler = client.get_default_handler()
handler.addFilter(ContextFilter())
setup_logging(handler)

# Also stream messages to the console (for local development)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
root_logger = logging.getLogger()
root_logger.addHandler(stream_handler)


def main(request: flask.Request):
    event = request.get_json()
    with open_dagster_pipes(
        params_loader=PipesMappingParamsLoader(event),
        message_writer=PipesLoggerMessageWriter(),
    ) as pipes:
        pipes.log.info(f"Version: {__version__}")
        event_value = event["some_parameter_value"]
        pipes.log.info(f"Hello, {event_value}!")
        pipes.report_asset_materialization(
            metadata={"some_metric": {"raw_value": event_value + 1, "type": "int"}},
            data_version="alpha",
        )
    return "OK", 200
