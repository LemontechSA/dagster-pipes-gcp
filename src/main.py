import logging
import typing

import flask
import google.cloud.logging
from dagster_pipes import PipesMappingParamsLoader, open_dagster_pipes
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging_v2.handlers import setup_logging

from version import __version__

EXECUTION_ID: typing.Optional[str] = None


class ContextFilter(logging.Filter):
    def filter(self, record):
        record.json_fields = {"execution_id": EXECUTION_ID}
        return True


client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client)
handler.addFilter(ContextFilter())
setup_logging(handler)

# client = google.cloud.logging.Client()
# logger = client.logger("dagster-pipes-gcp")


def main(request: flask.Request):
    trace_header = request.headers.get("X-Cloud-Trace-Context")
    trace = trace_header.split("/")[0]
    global EXECUTION_ID
    EXECUTION_ID = trace
    logging.info(f"Version: {__version__}")
    event = request.get_json()
    with open_dagster_pipes(params_loader=PipesMappingParamsLoader(event)) as pipes:
        event_value = event["some_parameter_value"]
        pipes.log.info(f"Hello, {event_value}!")
        pipes.report_asset_materialization(
            metadata={"some_metric": {"raw_value": event_value + 1, "type": "int"}},
            data_version="alpha",
        )
    return "OK", 200
