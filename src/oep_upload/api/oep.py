from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth
from requests import HTTPError

from oep_upload.config import get_settings, export_env_vars


# ======================================================================
# Shared API client (built from your config)
# ======================================================================
@dataclass(slots=True)
class OEPApiClient:
    base_url: str
    timeout_s: int
    token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    max_retries: int = 3
    retry_base_delay: float = 1.5

    def __post_init__(self):
        self.base_url = self.base_url.rstrip("/") + "/"
        self.session = requests.Session()
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Token {self.token}"
        self.session.headers.update(headers)
        self.auth = (
            HTTPBasicAuth(self.username, self.password)
            if self.username and self.password
            else None
        )

    def join(self, *parts: str) -> str:
        return self.base_url + "/".join(p.strip("/") for p in parts if p is not None)

    def get_json(
        self, *parts: str, params: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        url = self.join(*parts)
        r = self.session.get(url, params=params, auth=self.auth, timeout=self.timeout_s)
        r.raise_for_status()
        return r.json()

    def post_json(
        self,
        *parts: str,
        payload: Any = None,
        timeout_override: Optional[int] = None,
        retry_for_5xx: bool = True,
    ) -> Tuple[int, Dict[str, Any]]:
        url = self.join(*parts)
        t = timeout_override or max(120, self.timeout_s)
        attempts = self.max_retries if retry_for_5xx else 1

        for attempt in range(1, attempts + 1):
            try:
                resp = self.session.post(url, json=payload, auth=self.auth, timeout=t)
                status = resp.status_code
                try:
                    data = resp.json()
                except Exception:
                    data = {"raw": resp.text}
                if status >= 500 and retry_for_5xx:
                    raise RuntimeError(f"Server {status}: {data}")
                return status, data
            except Exception as e:
                if attempt == attempts:
                    raise
                delay = self.retry_base_delay**attempt
                print(f"POST retry {attempt}/{attempts} in {delay:.1f}s due to: {e}")
                time.sleep(delay)

    @classmethod
    def from_settings(cls):
        s = get_settings()
        export_env_vars(s)  # keep legacy env consumers happy

        base = str(s.endpoint.api_base_url).rstrip("/") + "/"
        timeout = int(s.api.timeout_s)
        token = s.effective_api_token or None

        # Try to pick up optional basic auth from config if present
        # (robust to missing sections/attrs)
        auth_cfg = getattr(s, "auth", None)
        api_cfg = getattr(s, "api", None)
        user = getattr(auth_cfg, "username", None) or getattr(api_cfg, "username", None)
        pwd = getattr(auth_cfg, "password", None) or getattr(api_cfg, "password", None)

        maxr = int(getattr(getattr(s, "upload", None), "max_retries", 3))
        base_delay = float(getattr(getattr(s, "upload", None), "retry_base_delay", 1.5))

        return cls(
            base_url=base,
            timeout_s=timeout,
            token=token,
            username=user,
            password=pwd,
            max_retries=maxr,
            retry_base_delay=base_delay,
        )


# Singleton client you can import if you like
_api_client = OEPApiClient.from_settings()


# ======================================================================
# Tables service (used by your upload tool)
# ======================================================================
class TablesService:
    """
    Endpoints used:
      GET  <base>/schema/{schema}/tables/{table}
      GET  <base>/schema/{schema}/tables/{table}/meta
      POST <base>/schema/{schema}/tables/{table}/rows/new {query: [...]}
    """

    def __init__(self, client: OEPApiClient | None = None):
        self.client = client or _api_client

    def get_table_info(self, schema: str, table: str) -> Dict[str, Any]:
        return self.client.get_json("schema", schema, "tables", table)

    def get_table_meta(self, schema: str, table: str) -> Dict[str, Any]:
        return self.client.get_json("schema", schema, "tables", table, "meta")

    def post_rows(
        self, schema: str, table: str, rows: List[Dict[str, Any]]
    ) -> Tuple[int, Dict[str, Any]]:
        status, payload = self.client.post_json(
            "schema",
            schema,
            "tables",
            table,
            "rows",
            "new",
            payload={"query": rows},
            timeout_override=None,  # will default to max(120, timeout)
            retry_for_5xx=True,
        )
        return status, payload


# ======================================================================
# Datasets service
# ======================================================================
class DatasetsService:
    """
    Endpoints used:
      GET  <base>/v0/datasets/{dataset_name}/
      POST <base>/v0/datasets/
      POST <base>/v0/datasets/{dataset_name}/assign-tables/
    """

    def __init__(self, client: OEPApiClient | None = None):
        self.client = client or _api_client

    def get_dataset(self, name: str) -> Optional[Dict[str, Any]]:
        try:
            return self.client.get_json("v0", "datasets", f"{name}", "")
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
        payload = {"name": name, "title": title, "description": description}
        if at_id:
            payload["at_id"] = at_id
        status, data = self.client.post_json(
            "v0", "datasets", "", payload=payload, retry_for_5xx=True
        )
        if status not in (200, 201):
            raise RuntimeError(f"Create dataset failed: {status} {data}")
        return data

    def assign_tables(
        self, dataset_name: str, tables: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        payload = {"dataset_name": dataset_name, "tables": tables}
        status, data = self.client.post_json(
            "v0",
            "datasets",
            f"{dataset_name}",
            "assign-tables",
            "",
            payload=payload,
            retry_for_5xx=True,
        )
        if status not in (200, 201, 202):
            raise RuntimeError(f"Assign tables failed: {status} {data}")
        return data


# ======================================================================
# High-level orchestration
# ======================================================================
def _resolve_oem_path(explicit: str | Path | None) -> Path:
    """
    Resolve datapackage/oem path:
      - if explicit path given → use it
      - else use config.paths.datapackage_file
    """
    s = get_settings()
    export_env_vars(s)
    if explicit:
        p = Path(explicit).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"OEM file not found: {p}")
        return p

    # from config
    cfg = getattr(s, "paths", None)
    dp = getattr(cfg, "datapackage_file", None)
    if not dp:
        raise SystemExit(
            "No datapackage file configured. Set paths.datapackage_file or pass a path."
        )
    p = Path(dp).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"OEM file not found: {p}")
    return p


def ensure_dataset_from_datapackage(
    datapackage_path: str | Path | None = None,
    *,
    assign_resources: bool = True,
    default_schema: Optional[str] = None,
    client: OEPApiClient | None = None,
) -> Dict[str, Any]:
    """
    1) Read OEMetadata v2 datapackage.json (from arg or configured path).
    2) Ensure dataset exists (GET/POST).
    3) Optionally assign 'resources' to dataset using {schema,name}.
    """
    client = client or _api_client
    ds_service = DatasetsService(client)

    p = _resolve_oem_path(datapackage_path)
    dp = json.loads(p.read_text(encoding="utf-8"))

    name = dp.get("name")
    if not name:
        raise ValueError("datapackage.json is missing required field: 'name'")
    title = dp.get("title") or name
    description = dp.get("description") or f"Dataset {name}"
    at_id = dp.get("@id")

    ds = ds_service.get_dataset(name)
    if not ds:
        ds = ds_service.create_dataset(
            name=name, title=title, description=description, at_id=at_id
        )

    if assign_resources:
        s = get_settings()
        export_env_vars(s)
        schema_default = default_schema or getattr(
            getattr(s, "upload", None), "default_schema", "model_draft"
        )

        resources = dp.get("resources") or []
        tables_for_api: List[Dict[str, str]] = []
        for res in resources:
            # Prefer explicit schema in OEM if present; else use default.
            schema = (
                (res.get("schema") or schema_default)
                if isinstance(res, dict)
                else schema_default
            )
            tname = res.get("name") if isinstance(res, dict) else None
            if schema and tname:
                tables_for_api.append({"schema": schema, "name": tname})

        if tables_for_api:
            ds_service.assign_tables(dataset_name=name, tables=tables_for_api)

    final = ds_service.get_dataset(name)
    return final or ds
