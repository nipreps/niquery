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

import functools
from collections.abc import Callable
from typing import Any, TypeVar, cast

import click

from niquery.utils.optpckg import have_datalad

F = TypeVar("F", bound=Callable[..., Any])

_datalad_req_msg = "DataLad or its dependencies are not available."


def require_datalad(func: F) -> F:
    """Decorator for functions that require DataLad.

    Checks whether DataLad and its required dependencies are available in the
    environment: if DataLad or its required system dependencies are not
    available, calling the decorated function will raise a :exc:`RuntimeError`
    with an informative message.

    Parameters
    ----------
    func : :obj:`~collections.abc.Callable`
        The function to be decorated. This function should depend on DataLad
        being installed and the ``git`` and ``git-annex`` system tools being
        available.

    Returns
    -------
    :obj:`~collections.abc.Callable`
        The wrapped function, which will only execute if DataLad is available.

    Raises
    ------
    :exc:`RuntimeError`
        If DataLad or its dependencies are not available when the decorated
        function is called.

    Examples
    --------
    .. code-block:: python

        @require_datalad
        def my_function(...):
            ...

    See Also
    --------
    :obj:`~niquery.utils.optpckg.have_datalad`
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not have_datalad():
            raise RuntimeError(_datalad_req_msg)
        return func(*args, **kwargs)

    return cast(F, wrapper)


def require_datalad_click(func: F) -> F:
    """Decorator for click command functions that require DataLad.

    If DataLad or its required system dependencies are not available, calling
    the decorated function will raise a :exc:`RuntimeError` with an informative
    message.

    Checks whether DataLad and its required dependencies are available in the
    environment: if DataLad or its required system dependencies are not
    available, it raises a :exc:`~click.ClickException` with a descriptive error
    message.

    Returns
    -------
    :obj:`~collections.abc.Callable`
        The wrapped click command function, which will only execute if DataLad
        is available.

    Raises
    ------
    :exc:`~click.ClickException`
        If DataLad or its dependencies are not available.

    Examples
    --------
    .. code-block:: python

        @click.command()
        @require_datalad_click
        def my_command():
            ...

    See Also
    --------
    :obj:`~niquery.utils.decorators.require_datalad`
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return require_datalad(func)(*args, **kwargs)
        except RuntimeError as e:
            raise click.ClickException(str(e)) from e

    return cast(F, wrapper)
