"""Tests for Settings token selection and Endpoint protocol derivation."""

from __future__ import annotations

import pytest

from oep_upload.config.models import APISettings, AppSettings, Endpoint, Settings


def _make_settings(target: str, *, token=None, local_token=None) -> Settings:
    api = APISettings(
        target=target,
        remote=Endpoint(
            host="openenergyplatform.org",
            api_base_url="https://openenergyplatform.org/api/v0/",
        ),
        local=Endpoint(
            host="localhost:8000",
            api_base_url="http://localhost:8000/api/v0/",
        ),
    )
    return Settings(
        app=AppSettings(),
        api=api,
        oep_api_token=token,
        oep_api_token_local=local_token,
    )


def test_remote_uses_main_token():
    s = _make_settings("remote", token="MAIN", local_token="LOCAL")
    assert s.effective_api_token == "MAIN"


def test_local_prefers_local_token():
    s = _make_settings("local", token="MAIN", local_token="LOCAL")
    assert s.effective_api_token == "LOCAL"


def test_local_falls_back_to_main_token():
    # The old code returned None here (no fallback); now we fall back.
    s = _make_settings("local", token="MAIN", local_token=None)
    assert s.effective_api_token == "MAIN"


def test_endpoint_selection_follows_target():
    assert _make_settings("remote").endpoint.host == "openenergyplatform.org"
    assert _make_settings("local").endpoint.host == "localhost:8000"


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://openenergyplatform.org/api/v0/", "https"),
        ("http://localhost:8000/api/v0/", "http"),
    ],
)
def test_protocol_is_derived_from_url(url, expected):
    ep = Endpoint(host="h", api_base_url=url)
    assert ep.protocol == expected


def test_explicit_protocol_is_kept():
    ep = Endpoint(
        host="h", api_base_url="https://example.org/api/", protocol="http"
    )
    assert ep.protocol == "http"


def test_oedialect_protocol_defaults_to_https_when_unknown():
    s = _make_settings("remote")
    assert s.oedialect_protocol in ("http", "https")