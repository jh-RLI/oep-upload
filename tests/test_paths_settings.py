"""Tests for PathsSettings expansion, defaults and resolution."""

from __future__ import annotations

from pathlib import Path

from oep_upload.config.models import PathsSettings


def test_defaults_are_safe_for_fresh_users():
    p = PathsSettings()
    assert p.root == "data"
    assert p.data_dir is None
    assert p.datapackage_file is None
    # With no data_dir, the data dir is the root itself.
    assert p.resolved_data_dir == p.resolved_root
    # No datapackage file configured -> None (not a crash).
    assert p.resolved_datapackage_file is None


def test_empty_strings_are_normalized():
    p = PathsSettings(root="", data_dir="   ", datapackage_file="")
    # root always keeps a usable value...
    assert p.root == "data"
    # ...the optional ones collapse to None.
    assert p.data_dir is None
    assert p.datapackage_file is None


def test_whitespace_is_stripped():
    p = PathsSettings(root="  /a/b  ")
    assert p.root == "/a/b"


def test_user_home_is_expanded():
    p = PathsSettings(root="~/datasets")
    assert "~" not in p.root
    assert p.root == str(Path.home() / "datasets")


def test_env_vars_are_expanded(monkeypatch):
    monkeypatch.setenv("MYDATA", "/srv/data")
    p = PathsSettings(root="$MYDATA/sets")
    assert p.root == "/srv/data/sets"


def test_relative_data_dir_is_joined_to_root():
    p = PathsSettings(root="/a/b", data_dir="sub")
    assert p.resolved_data_dir == Path("/a/b/sub")


def test_absolute_data_dir_overrides_root():
    p = PathsSettings(root="/a/b", data_dir="/c/d")
    assert p.resolved_data_dir == Path("/c/d")


def test_relative_datapackage_file_is_joined_to_data_dir():
    p = PathsSettings(root="/a/b", data_dir="sub", datapackage_file="dp.json")
    assert p.resolved_datapackage_file == Path("/a/b/sub/dp.json")


def test_absolute_datapackage_file_overrides():
    p = PathsSettings(root="/a/b", datapackage_file="/elsewhere/dp.json")
    assert p.resolved_datapackage_file == Path("/elsewhere/dp.json")