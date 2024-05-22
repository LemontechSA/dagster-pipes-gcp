import logging

import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler

client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client)

logger = logging.getLogger("dagster-pipes-gcp")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

__version__ = "0.0.0"


def main(request):
    logger.info(__version__)
    logger.info("Hello world!")
    return "boo"
