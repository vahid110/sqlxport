# tests/unit/test_preview.py

import os
import tempfile
from click.testing import CliRunner
from sqlxport.cli.cmd_preview import preview

def test_preview_local_csv(monkeypatch):
    content = "col1,col2\n1,foo\n2,bar\n"
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w")
    tmp_file.write(content)
    tmp_file.close()

    monkeypatch.setenv("SQLXPORT_ENV_PATH", "")  # Just in case

    result = CliRunner().invoke(preview, ["--local-file", tmp_file.name])
    assert result.exit_code == 0
    assert "foo" in result.output or "bar" in result.output

    os.unlink(tmp_file.name)
