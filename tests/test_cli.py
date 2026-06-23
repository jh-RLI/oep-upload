"""Tests for the CLI scaffolding commands.

These must work without any valid config or token (that's the point of `init`),
so importing the cli module must not trigger settings loading.
"""

from __future__ import annotations

from oep_upload import cli


def test_init_writes_templates(tmp_path):
    rc = cli.cmd_init(str(tmp_path), force=False)
    assert rc == 0
    assert (tmp_path / "settings.local.yaml").is_file()
    assert (tmp_path / ".env").is_file()
    text = (tmp_path / "settings.local.yaml").read_text(encoding="utf-8")
    assert "paths:" in text and "target:" in text


def test_init_skips_existing_without_force(tmp_path):
    (tmp_path / ".env").write_text("OEP_API_TOKEN=keepme\n", encoding="utf-8")
    cli.cmd_init(str(tmp_path), force=False)
    assert "keepme" in (tmp_path / ".env").read_text(encoding="utf-8")


def test_init_force_overwrites(tmp_path):
    (tmp_path / ".env").write_text("OEP_API_TOKEN=old-token\n", encoding="utf-8")
    cli.cmd_init(str(tmp_path), force=True)
    assert "old-token" not in (tmp_path / ".env").read_text(encoding="utf-8")


def test_main_init_via_argv(tmp_path):
    rc = cli.main(["init", "--dir", str(tmp_path)])
    assert rc == 0
    assert (tmp_path / "settings.local.yaml").is_file()
    assert (tmp_path / ".env").is_file()