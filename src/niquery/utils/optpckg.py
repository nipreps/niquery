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


def have_datalad() -> bool:
    """Check if DataLad and its required system dependencies are available.

    The required system dependencies include ``git`` and ``git-annex``.

    Returns
    -------
    :obj:`bool`
        :obj:`True` if DataLad and its required system dependencies are
        available, :obj:`False` otherwise.
    """

    try:
        import shutil

        # Check that git and git-annex are in the PATH
        if shutil.which("git") is None or shutil.which("git-annex") is None:
            logging.warning("DataLad cannot be used: git or git-annex not found")
            return False
        # Try to instantiate a Dataset with a temporary path
        import tempfile

        from datalad.api import Dataset  # type: ignore[import-untyped]

        with tempfile.TemporaryDirectory() as tmpdir:
            _ = Dataset(tmpdir)
        return True
    except (ImportError, AttributeError, FileNotFoundError) as e:
        logging.warning(f"DataLad availability check failed: {e}")
        return False
    except Exception as e:
        # Log unexpected exceptions
        logging.error(f"Unexpected error in DataLad check: {e}")
        return False
