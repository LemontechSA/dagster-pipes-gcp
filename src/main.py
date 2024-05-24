import google.cloud.logging

from .version import __version__

client = google.cloud.logging.Client()
logger = client.logger("dagster-pipes-gcp")


def main(request):
    global_log_fields = {}
    trace_header = request.headers.get("X-Cloud-Trace-Context")
    trace = trace_header.split("/")
    global_log_fields["logging.googleapis.com/trace"] = (
        f"projects/jasper-ginn-dagster/traces/{trace[0]}"
    )
    logger.log_struct({"severity": "INFO", "message": request["name"], **global_log_fields})
    logger.log_struct({"severity": "INFO", "message": __version__, **global_log_fields})
    logger.log_struct({"severity": "INFO", "message": "Hello world!", **global_log_fields})
    return "boo"
