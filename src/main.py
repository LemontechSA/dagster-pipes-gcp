import flask
import google.cloud.logging
from dagster_pipes import PipesMappingParamsLoader, open_dagster_pipes

from pipes_utils import PipesLoggerMessageWriter
from version import __version__

client = google.cloud.logging.Client()
client.setup_logging()


def main(request: flask.Request):
    event = request.get_json()
    trace_header = request.headers.get("X-Cloud-Trace-Context")
    print(trace_header)
    if trace_header is None:
        trace = "troio"
    else:
        trace = trace_header.split("/")[0]
    with open_dagster_pipes(
        params_loader=PipesMappingParamsLoader(event),
        message_writer=PipesLoggerMessageWriter(
            trace=f"projects/jasper-ginn-dagster/traces/{trace}"
        ),
    ) as pipes:
        pipes.log.info(f"Version: {__version__}")
        event_value = event["some_parameter_value"]
        pipes.log.info(f"Hello, {event_value}!")
        pipes.report_asset_materialization(
            metadata={"some_metric": {"raw_value": event_value + 1, "type": "int"}},
            data_version="alpha",
        )
    return "OK", 200
