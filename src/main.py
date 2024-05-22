import logging

import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

client = google.cloud.logging.Client()
handler = CloudLoggingHandler(client)

logger = logging.getLogger("dagster-pipes-gcp")
format = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(format)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

__version__ = "0.0.0"


def main(request):
    logger.info(__version__)
    logger.info("Hello world!")
    return "boo"
