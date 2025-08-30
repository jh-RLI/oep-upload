from pathlib import Path
import json
from typing import Any, Dict, List, Optional
from requests import get, post
from requests.auth import HTTPBasicAuth
from requests import HTTPError


class OEPDatasetsService:
    """
    api_base should be something like:
      - "https://your-oep.example.org/api"  (if your urls.py includes path('api/', ...))
      - or "https://your-oep.example.org"   (if you expose v0/ at the root)
    Endpoints used:
      GET  {api_base}/v0/datasets/{dataset_name}/
      POST {api_base}/v0/datasets/
      POST {api_base}/v0/datasets/{dataset_name}/assign-tables/
    """

    def __init__(
        self,
        api_base: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
    ):
        self.api_base = api_base.rstrip("/")
        self.auth = HTTPBasicAuth(username, password) if username and password else None
        self.token = token

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.token:
            h["Authorization"] = f"Token {self.token}"
        return h

    # --- API calls -----------------------------------------------------------
    def get_dataset(self, name: str) -> Optional[Dict[str, Any]]:
        url = f"{self.api_base}/v0/datasets/{name}/"
        try:
            r = get(url, headers=self._headers(), auth=self.auth, timeout=30)
            r.raise_for_status()
            return r.json()
        except HTTPError as e:
            if (
                getattr(e, "response", None) is not None
                and e.response.status_code == 404
            ):
                return None
            raise

    def create_dataset(
        self, name: str, title: str, description: str, at_id: Optional[str] = None
    ) -> Dict[str, Any]:
        url = f"{self.api_base}/v0/datasets/"
        payload = {"name": name, "title": title, "description": description}
        if at_id:
            payload["at_id"] = at_id  # optional, if your view accepts it
        r = post(url, json=payload, headers=self._headers(), auth=self.auth, timeout=60)
        r.raise_for_status()
        return r.json()

    def assign_tables(
        self, dataset_name: str, tables: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        url = f"{self.api_base}/v0/datasets/{dataset_name}/assign-tables/"
        payload = {"dataset_name": dataset_name, "tables": tables}
        r = post(url, json=payload, headers=self._headers(), auth=self.auth, timeout=60)
        r.raise_for_status()
        return r.json()


# --- High-level orchestration -------------------------------------------------
def ensure_dataset_from_datapackage(
    api_base: str,
    datapackage_path: str | Path,
    *,
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
    assign_resources: bool = True,
) -> Dict[str, Any]:
    """
    1) Read OEMetadata v2 datapackage.json.
    2) Look up dataset via GET /v0/datasets/{name}/.
    3) If missing → POST /v0/datasets/ to create.
    4) Optionally map datapackage 'resources' to {"schema","name"} and assign via /assign-tables/.
    """
    dp = json.loads(Path(datapackage_path).read_text(encoding="utf-8"))

    name = dp.get("name")
    if not name:
        raise ValueError("datapackage.json is missing required field: 'name'")
    title = dp.get("title") or name
    description = dp.get("description") or f"Dataset {name}"
    at_id = dp.get("@id")  # optional

    client = OEPDatasetsService(
        api_base, username=username, password=password, token=token
    )

    ds = client.get_dataset(name)
    if not ds:
        ds = client.create_dataset(
            name=name, title=title, description=description, at_id=at_id
        )

    if assign_resources:
        resources = dp.get("resources") or []
        tables_for_api: List[Dict[str, str]] = []
        for res in resources:
            # Expect OEMetadata v2 resource entries to expose schema+name.
            schema = "model_draft"
            tname = res.get("name")
            if schema and tname:
                tables_for_api.append({"schema": schema, "name": tname})
        if tables_for_api:
            client.assign_tables(dataset_name=name, tables=tables_for_api)

    # Return the fresh server copy
    final = client.get_dataset(name)
    return final or ds


# --- Example usage ------------------------------------------------------------
if __name__ == "__main__":
    API_BASE = (
        "https://your-oep.example.org/api"  # or without /api if you mount v0 at root
    )
    USERNAME = "user"
    PASSWORD = "pass"
    TOKEN = None

    dataset = ensure_dataset_from_datapackage(
        api_base=API_BASE,
        datapackage_path="datapackage.json",
        username=USERNAME,
        password=PASSWORD,
        token=TOKEN,
        assign_resources=True,
    )
    print(f"Dataset ensured: {dataset.get('name')} (uuid: {dataset.get('uuid')})")
