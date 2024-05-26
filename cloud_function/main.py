import flask
import google.cloud.logging
import google.cloud.storage
from dagster_pipes import PipesContext, PipesMappingParamsLoader, open_dagster_pipes
from fn_pipes import PipesCloudStorageMessageWriter  # , PipesCloudLoggerMessageWriter,
from fn_utils import get_fake_data
from version import __version__

client = google.cloud.logging.Client()
client.setup_logging()


def main(request: flask.Request):
    event = request.get_json()
    trace_header = request.headers.get("X-Cloud-Trace-Context") or "local"
    trace = trace_header.split("/")[0]
    with open_dagster_pipes(
        params_loader=PipesMappingParamsLoader(event),
        message_writer=PipesCloudStorageMessageWriter(
            client=google.cloud.storage.Client(),
        ),
    ) as pipes:
        pipes.log.info(f"Cloud function version: {__version__}")
        pipes.log.info(f"Cloud function trace: {trace}")
        dl_bucket = event["dl_bucket"]
        pipes.log.debug(f"Storing data in bucket {dl_bucket}")
        table_location = f"{dl_bucket}/{pipes.asset_key}"
        pipes.log.debug(f"Writing data to {table_location}")
        df = get_fake_data()
        df.write_delta(table_location, mode="append")
        pipes.report_asset_materialization(
            metadata={"table_location": table_location},
            data_version="alpha",
        )
    # Force the context to be reinitialized on the next request
    # see: https://github.com/dagster-io/dagster/issues/22094
    PipesContext._instance = None
    return "OK", 200
