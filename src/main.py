import logging

import google.cloud.logging

client = google.cloud.logging.Client()

handler = client.get_default_handler()
logger = logging.getLogger("cloudLogger")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

__version__ = "0.0.0"


def main(request):
    logger.info(__version__)
    logger.info("Hello world!")
    return "boo"
