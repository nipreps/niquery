# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
#
# Copyright The NiPreps Developers <nipreps@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# We support and encourage derived works from this project, please read
# about our expectations at
#
#     https://www.nipreps.org/community/licensing/
#

import os

import click
import pytest

from niquery.cli.utils import force_output, verify_output_path


def test_force_output(monkeypatch):
    # Make a dummy command and decorate it
    @force_output
    def dummy():
        pass

    # The click.option decorator adds parameters onto the function for click to use
    # We can check that click recognizes the --force flag
    params = [p for p in getattr(dummy, "__click_params__", []) if isinstance(p, click.Option)]
    assert any("--force" in p.opts and "-f" in p.opts for p in params)
    assert any(p.is_flag for p in params)
    assert any(p.default is False for p in params)
    assert any("Allow overwriting output files" in p.help for p in params)


def test_verify_output_path(tmp_path):
    # Create file
    file_path = tmp_path / "test.txt"
    file_path.write_text("content")

    # Should raise if overwrite is False
    with pytest.raises(click.ClickException) as excinfo:
        verify_output_path(file_path, overwrite=False)
    assert "exists" in str(excinfo.value)

    # Should NOT raise if overwrite is True
    verify_output_path(file_path, overwrite=True)  # Should not raise

    os.remove(file_path)

    # Create non-empty directory
    dir_path = tmp_path / "test_dir"
    dir_path.mkdir()
    (dir_path / "file.txt").write_text("something")

    # Should raise if overwrite is False
    with pytest.raises(click.ClickException) as excinfo:
        verify_output_path(dir_path, overwrite=False)
    assert "not empty" in str(excinfo.value)

    # Should NOT raise if overwrite is True
    verify_output_path(dir_path, overwrite=True)  # Should not raise

    # Non-existent file (should not raise)
    file_path = tmp_path / "no_file.txt"
    verify_output_path(file_path, overwrite=False)

    # Empty directory (should not raise)
    dir_path = tmp_path / "empty_dir"
    dir_path.mkdir()
    verify_output_path(dir_path, overwrite=False)
