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

import logging

from niquery.utils.logging import _create_log_file_rootname, configure_logging


def test_create_log_file_rootname(monkeypatch):
    class DummyMeta(dict):
        def __init__(self):
            super().__init__(Name="custompkg")

    monkeypatch.setattr(
        "niquery.utils.logging.importlib.metadata.metadata", lambda pkg: DummyMeta()
    )
    root = _create_log_file_rootname("myfunc")
    assert root == "custompkg_myfunc"


def test_configure_logging(tmp_path, monkeypatch):
    class DummyMeta(dict):
        def __init__(self):
            super().__init__(Name="mypkg")

    monkeypatch.setattr(
        "niquery.utils.logging.importlib.metadata.metadata", lambda pkg: DummyMeta()
    )

    # Keep original handlers to restore later (avoid side effects across test session)
    orig_handlers = logging.root.handlers[:]
    try:
        configure_logging(tmp_path, "run")
        # Emit a log message
        logging.info("hello world")

        expected_logfile = tmp_path / "mypkg_run.log"
        assert expected_logfile.exists()
        content = expected_logfile.read_text()
        assert "hello world" in content

        # Expect at least one stream and one file handler configured
        handler_types = {type(h).__name__ for h in logging.root.handlers}
        assert "FileHandler" in handler_types
        assert "StreamHandler" in handler_types
    finally:
        # Restore original handlers
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)
        for h in orig_handlers:
            logging.root.addHandler(h)
