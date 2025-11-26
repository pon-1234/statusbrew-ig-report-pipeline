from __future__ import annotations

from typing import Optional

from google.cloud import secretmanager


def fetch_secret(secret_name: str, project_id: Optional[str] = None) -> str:
    client = secretmanager.SecretManagerServiceClient()
    if "/" in secret_name:
        name = secret_name
    else:
        if not project_id:
            raise ValueError("project_id is required when secret_name is not a resource name")
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")
