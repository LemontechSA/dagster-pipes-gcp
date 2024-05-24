import typing

import google.cloud.logging
import google.oauth2.id_token
import google.oauth2.service_account
import httpx
import requests
import tenacity
from google.auth.transport.requests import AuthorizedSession, Request


class NoLogsException(Exception): ...


def _with_id_token(url: str, timeout: int) -> httpx.Response:
    """Use credentials set on GOOGLE_APPLICATION_CREDENTIALS environment variable to invoke google cloud function"""
    auth_req = Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, url)
    headers = {"Authorization": "bearer " + id_token, "Content-Type": "application/json"}
    resp = httpx.get(url=url, headers=headers, timeout=timeout)
    return resp


def _with_service_account(
    url: str, service_account_file: str, timeout: int
) -> requests.models.Response:
    """Use service account credentials from a specific file to invoke the cloud function"""
    creds = google.oauth2.service_account.IDTokenCredentials.from_service_account_file(
        service_account_file, target_audience=url
    )
    authed_session = AuthorizedSession(creds)
    return authed_session.get(url, timeout=timeout)


def invoke_cloud_function(
    url: str, service_account_file: typing.Optional[str] = None, timeout: int = 120
) -> typing.Union[httpx.Response, requests.models.Response]:
    """Invoke a Google Cloud Function"""
    # TODO: take request input parameters
    if service_account_file is not None:
        resp = _with_service_account(url, service_account_file, timeout)
    else:  # Fallback. Use GOOGLE_APPLICATION_CREDENTIALS
        resp = _with_id_token(url, timeout)
    return resp


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5), wait=tenacity.wait_exponential_jitter(initial=1)
)
def get_execution_logs(trace_id: str):
    """Lists the most recent entries for a given logger."""
    logging_client = google.cloud.logging.Client()

    out = []
    for entry in logging_client.list_entries(
        filter_=f'resource.type="cloud_function" AND jsonPayload."logging.googleapis.com/trace"="projects/jasper-ginn-dagster/traces/{trace_id}"'
    ):
        out.append(entry.payload)
    if len(out) == 0:
        raise NoLogsException(f"No logs found for trace id={trace_id}")
    return out
