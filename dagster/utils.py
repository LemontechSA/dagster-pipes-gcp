import google.cloud.logging
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account


def invoke_cloud_function(url: str, service_account_file: str):
    creds = service_account.IDTokenCredentials.from_service_account_file(
        service_account_file, target_audience=url
    )
    authed_session = AuthorizedSession(creds)
    return authed_session.get(url)


def get_execution_logs(logger_name: str, trace_id: str):
    """Lists the most recent entries for a given logger."""
    logging_client = google.cloud.logging.Client()
    logger = logging_client.logger(logger_name)

    out = []
    for entry in logger.list_entries(
        filter_=f'jsonPayload."logging.googleapis.com/trace"="projects/jasper-ginn-dagster/traces/{trace_id}"'
    ):
        out.append(entry.payload)
    return out
