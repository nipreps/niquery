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

import importlib.metadata
import logging
import os
from pathlib import Path

UNDERSCORE = "_"


def _create_log_file_rootname(function_name: str) -> str:
    """Create log file root name.

    Creates the log file root name by prefixing the package name to the given
    function name.

    Parameters
    ----------
    function_name : :obj:`str`
        Function name.

    Returns
    -------
    :obj:`str`
        Log file root name.
    """

    return importlib.metadata.metadata("niquery")["Name"] + UNDERSCORE + function_name


def configure_logging(dirname: Path, filename: str) -> None:
    """Configure logging.

    Parameters
    ----------
    dirname : :obj:`~pathlib.Path`
        Directory where to save the logfile.
    filename : :obj:`~pathlib.Path`
        Filename. Will be appended to the package name to create the log
        filename.
    """

    file_rootname = _create_log_file_rootname(filename)

    root = logging.getLogger()

    fmt = "%(asctime)s - %(levelname)s - %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    # Prepare file handler
    file_path = f"{dirname}/{file_rootname}.log"
    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))

    # Add file handler if not already attached
    if not any(
        isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == file_path
        for h in root.handlers
    ):
        root.addHandler(file_handler)

    root.setLevel(logging.INFO)

    # Only add StreamHandler if NOT running under pytest
    if "PYTEST_CURRENT_TEST" not in os.environ:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))
        root.addHandler(stream_handler)
