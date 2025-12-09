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

import re
from pathlib import Path


def filter_non_conforming_ds(dirname: Path) -> dict:
    r"""Filter non-conforming datasets based on their name.

    Consider only files that have the 'ds\d{6}\.tsv' pattern (e.g.
    'ds000006.tsv', 'ds000021.tsv', etc.)

    Parameters
    ----------
    dirname : :obj:`~pathlib.Path`
        Directory where dataset files are located.

    Returns
    -------
    :obj:`dict`
        Pairs of dataset names and absolute paths.
    """

    pattern = bids_dataset_name_pattern()

    return {
        entry.stem: entry
        for entry in dirname.iterdir()
        if entry.is_file() and pattern.fullmatch(entry.name)
    }


def bids_dataset_name_pattern() -> re.Pattern[str]:
    r"""Return the compiled regex pattern to identify BIDS dataset filenames.

    Compiles a specific regex pattern designed to match filenames associated
    with BIDS datasets. The pattern is structured to match strings that have
    'ds\d{6}\.tsv' pattern (e.g. 'ds000006.tsv', 'ds000021.tsv', etc.): i.e. the
    'ds' prefix is followed by exactly six digits and ends with the '.tsv'
    extension.

    Returns
    -------
    :obj:`~re.Pattern[str]`
        A compiled regex pattern matching BIDS dataset filenames.
    """

    return re.compile(r"ds\d{6}\.tsv")
