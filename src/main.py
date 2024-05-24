import flask

# import google.cloud.logging
from dagster_pipes import PipesMappingParamsLoader, open_dagster_pipes

# from .version import __version__

# client = google.cloud.logging.Client()
# logger = client.logger("dagster-pipes-gcp")


def main(request: flask.Request):
    event = request.get_json()
    with open_dagster_pipes(params_loader=PipesMappingParamsLoader(event)) as pipes:
        event_value = event["some_parameter_value"]
        pipes.log.info(f"Hello, {event_value}!")
        pipes.report_asset_materialization(
            metadata={"some_metric": {"raw_value": event_value + 1, "type": "int"}},
            data_version="alpha",
        )
    return "OK", 200
