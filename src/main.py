import google.cloud.logging

client = google.cloud.logging.Client()
logger = client.logger("dagster-pipes-gcp")

__version__ = "0.0.0"


def main(request):
    logger.log_struct({"severity": "INFO", "message": __version__})
    logger.log_struct({"severity": "INFO", "message": "Hello world!"})
    return "boo"
