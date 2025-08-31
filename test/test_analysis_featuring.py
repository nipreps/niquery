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

import gzip

import pandas as pd
import pytest

from niquery.analysis.featuring import (
    extract_bold_features,
    get_nii_timepoints,
    get_nii_timepoints_s3,
)
from niquery.utils.attributes import DATASETID, FULLPATH, VOLS


class DummyBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class DummyS3:
    def __init__(self, data: bytes):
        self._data = data

    def get_object(self, Bucket, Key, Range):
        return {"Body": DummyBody(self._data)}


class DummyResponse:
    def __init__(self, status_code=200, content=b""):
        self.status_code = status_code
        self.content = content


class DummyNifti:
    def __init__(self, dim4):
        # header behaves like a dict with key "dim" where index 4 gives timepoints
        self.header = {"dim": [0, 0, 0, 0, dim4]}

    @staticmethod
    def from_stream(stream):
        # Ignore stream content; return header with controlled value
        return DummyNifti(dim4=123)


def test_get_nii_timepoints_s3_success(monkeypatch):
    # Provide valid gzip content (actual content is ignored by mocked nibabel)
    gz = gzip.compress(b"anybytes")
    monkeypatch.setattr("niquery.analysis.featuring.s3", DummyS3(gz))
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)

    n = get_nii_timepoints_s3("ds000001/path/file.nii.gz")
    assert n == 123


def test_get_nii_timepoints_success_and_error(monkeypatch):
    gz = gzip.compress(b"x")
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)

    def ok_get(url, headers):
        return DummyResponse(206, content=gz)

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", ok_get)
    assert get_nii_timepoints("http://example/file.nii.gz") == 123

    def bad_get(url, headers):
        return DummyResponse(404, content=b"")

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", bad_get)
    with pytest.raises(RuntimeError):
        get_nii_timepoints("http://example/missing.nii.gz")


def test_extract_bold_features(monkeypatch):
    # Prepare input dict: two datasets with small DataFrames
    df1 = pd.DataFrame(
        [{FULLPATH: "sub-01/func/a_bold.nii.gz"}, {FULLPATH: "sub-01/func/b_bold.nii.gz"}]
    )
    df2 = pd.DataFrame([{FULLPATH: "sub-02/func/c_bold.nii.gz"}])

    datasets = {"ds1": df1, "ds2": df2}

    def fake_get_nii_timepoints_s3(path_str):
        # The implementation under test passes Path(dataset_id) / Path(df.iloc[0][FULLPATH])
        # so we distinguish by dataset id in the path string.
        if "ds1" in path_str:
            return 50
        raise RuntimeError("Pretending that ds2 was not successful")

    monkeypatch.setattr(
        "niquery.analysis.featuring.get_nii_timepoints_s3", fake_get_nii_timepoints_s3
    )

    success, failures = extract_bold_features(datasets, max_workers=2)

    # Success contains ds1 with 2 records and VOLS set, and empty ds2
    assert list(success.keys()) == ["ds1", "ds2"]
    assert [rec[FULLPATH] for rec in success["ds1"]] == sorted(
        [r[FULLPATH] for _, r in df1.iterrows()]
    )
    assert all(rec[VOLS] == 50 for rec in success["ds1"])
    assert success["ds2"] == []

    # Failures contain ds2's only record
    assert failures == [{DATASETID: "ds2", FULLPATH: df2.iloc[0][FULLPATH]}]
