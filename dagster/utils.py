from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account


def invoke_cloud_function(url: str, service_account_file: str):
    creds = service_account.IDTokenCredentials.from_service_account_file(
        service_account_file, target_audience=url
    )
    authed_session = AuthorizedSession(creds)
    return authed_session.get(url)
