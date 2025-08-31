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

from niquery.data.utils import bids_dataset_name_pattern, filter_non_conforming_ds


def test_bids_dataset_name_pattern_matching():
    pattern = bids_dataset_name_pattern()
    assert isinstance(pattern, re.Pattern)

    assert pattern.fullmatch("ds000001.tsv")
    assert pattern.fullmatch("ds123456.tsv")
    assert not pattern.fullmatch("ds00001.tsv")  # 5 digits
    assert not pattern.fullmatch("ds000001.csv")  # wrong extension
    assert not pattern.fullmatch("sub-ds000001.tsv")  # prefix not allowed
    assert not pattern.fullmatch("ds000001.tsv.bak")  # suffix not allowed
    assert not pattern.fullmatch("DS000001.tsv")  # case-sensitive


def test_filter_non_conforming_ds(tmp_path: Path):
    valid = ["ds000001.tsv", "ds000010.tsv", "ds123456.tsv"]
    invalid = [
        "ds00001.tsv",
        "ds000001.csv",
        "sub-ds000001.tsv",
        "ds000001.tsv.bak",
        "README",
    ]

    for name in valid + invalid:
        f = tmp_path / name
        f.write_text("x" if f.suffix else "")

    mapping = filter_non_conforming_ds(tmp_path)
    # Only valid stems should be present
    expected = {Path(v).stem for v in valid}
    assert set(mapping.keys()) == expected
    # Absolute paths to the files
    assert all(mapping[k].is_absolute() for k in mapping)
    # The stored paths should point to the right files
    assert {p.name for p in mapping.values()} == set(valid)
