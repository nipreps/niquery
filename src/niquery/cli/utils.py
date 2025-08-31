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

from pathlib import Path
from typing import Callable

import click


def force_output(f: Callable) -> Callable:
    """Create a click option to add the ``--force / -f`` flag to a command.

    Creates a Click command decorator to add an option that can be used to
    enable or disable the ability to forcefully overwrite existing output files,
    for example. The value of this option is stored in the Click context object
    and can be accessed during the command execution.

    Parameters
    ----------
    f : :obj:`Callable`
        The function to be wrapped with the Click option.

    Returns
    -------
    :obj:`Callable`
        The decorated function with the ``--force / -f`` flag.
    """

    return click.option(
        "--force",
        "-f",
        is_flag=True,
        default=False,
        help="Allow overwriting output files or writing to non-empty directories.",
    )(f)


def verify_output_path(path: Path, overwrite: bool = False):
    """Verify whether the output path already exists or is non-empty.

    Verifies whether a path exists (if a file) or whether it is not empty
    (if a directory) it; raise an error if it exists/not empty and ``overwrite``
    is ``False``.

    Parameters
    ----------
    path : :obj:`Path`
        Filename.
    overwrite : :obj:`bool`
        ``True`` to allow overwriting the file/writing to a non-empty directory.
    """

    if path.is_file() and not overwrite:
        raise click.ClickException(f"File {path} exists, but no overwriting has been forced")

    if path.is_dir() and any(path.iterdir()) and not overwrite:
        raise click.ClickException(
            f"Directory {path} not empty, but no overwriting has been forced"
        )
