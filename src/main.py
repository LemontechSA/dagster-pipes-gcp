import google.cloud.logging

from .version import __version__

client = google.cloud.logging.Client()
logger = client.logger("dagster-pipes-gcp")


def main(request):
    logger.log_struct({"severity": "INFO", "message": __version__})
    logger.log_struct({"severity": "INFO", "message": "Hello world!"})
    return "boo"
