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
    _get_nii_header_bytes,
    extract_volume_features,
    get_nii_header_s3,
    get_nii_header_url,
    get_nii_timepoints_s3,
    get_nii_timepoints_url,
)
from niquery.utils.attributes import DATASETID, FULLPATH, REMOTE, VOLS


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


class DummyNiftiHeader(dict):
    def __init__(self, dim4=123):
        super().__init__()
        self._dim4 = dim4

    def __getitem__(self, key):
        if key == "dim":
            return [0, 0, 0, 0, self._dim4]
        return super().__getitem__(key)


class DummyNifti:
    def __init__(self, dim4=123):
        # header behaves like a dict with key "dim" where index 4 gives timepoints
        self.header = DummyNiftiHeader(dim4)

    @staticmethod
    def from_stream(stream, dim4=123):
        # Ignore stream content; return header with controlled value
        return DummyNifti(dim4=dim4)


def test_get_nii_header(monkeypatch):
    gz = gzip.compress(b"anybytes")
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)
    header = _get_nii_header_bytes(gz)
    assert isinstance(header, DummyNiftiHeader)
    assert header["dim"][4] == 123


def test_get_nii_header_s3(monkeypatch):
    gz = gzip.compress(b"anybytes")
    monkeypatch.setattr("niquery.analysis.featuring.s3", DummyS3(gz))
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)
    header = get_nii_header_s3("mybucket", "ds000001/path/file.nii.gz")
    assert isinstance(header, DummyNiftiHeader)
    assert header["dim"][4] == 123


def test_get_nii_header_url(monkeypatch):
    gz = gzip.compress(b"anybytes")

    def ok_get(url, headers):
        return DummyResponse(206, content=gz)

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", ok_get)
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)
    header = get_nii_header_url("http://example/file.nii.gz")
    assert isinstance(header, DummyNiftiHeader)
    assert header["dim"][4] == 123

    def bad_get(url, headers):
        return DummyResponse(404, content=b"")

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", bad_get)
    with pytest.raises(RuntimeError):
        get_nii_header_url("http://example/missing.nii.gz")


def test_get_nii_timepoints_s3(monkeypatch):
    # Provide valid gzip content (actual content is ignored by mocked nibabel)
    gz = gzip.compress(b"anybytes")
    monkeypatch.setattr("niquery.analysis.featuring.s3", DummyS3(gz))
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)

    n = get_nii_timepoints_s3("mybucket", "ds000001/path/file.nii.gz")
    assert n == 123


def test_get_nii_voxelsize_s3(monkeypatch):
    # Provide valid gzip content (actual content is ignored by mocked nibabel)
    gz = gzip.compress(b"anybytes")
    monkeypatch.setattr("niquery.analysis.featuring.s3", DummyS3(gz))
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)

    n = get_nii_voxelsize_s3("mybucket", "ds000001/path/file.nii.gz")
    assert n == 123


def test_get_nii_anisotropy_s3(monkeypatch):
    # Provide valid gzip content (actual content is ignored by mocked nibabel)
    gz = gzip.compress(b"anybytes")
    monkeypatch.setattr("niquery.analysis.featuring.s3", DummyS3(gz))
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)

    n = get_nii_anisotropy_s3("mybucket", "ds000001/path/file.nii.gz")
    assert n == 123


def test_get_nii_timepoints_url(monkeypatch):
    gz = gzip.compress(b"anybytes")

    def ok_get(url, headers):
        return DummyResponse(206, content=gz)

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", ok_get)
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)

    assert get_nii_timepoints_url("http://example/file.nii.gz") == 123

    def bad_get(url, headers):
        return DummyResponse(404, content=b"")

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", bad_get)
    with pytest.raises(RuntimeError):
        get_nii_timepoints_url("http://example/missing.nii.gz")


def test_get_nii_voxel_size_url(monkeypatch):
    gz = gzip.compress(b"anybytes")

    def ok_get(url, headers):
        return DummyResponse(206, content=gz)

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", ok_get)
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)

    assert get_nii_voxel_size_url("http://example/file.nii.gz") == 123

    def bad_get(url, headers):
        return DummyResponse(404, content=b"")

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", bad_get)
    with pytest.raises(RuntimeError):
        get_nii_voxel_size_url("http://example/missing.nii.gz")


def test_get_nii_anisotropy_url(monkeypatch):
    gz = gzip.compress(b"anybytes")

    def ok_get(url, headers):
        return DummyResponse(206, content=gz)

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", ok_get)
    monkeypatch.setattr("niquery.analysis.featuring.nb.Nifti1Image", DummyNifti)

    assert get_nii_anisotropy_url("http://example/file.nii.gz") == 123

    def bad_get(url, headers):
        return DummyResponse(404, content=b"")

    monkeypatch.setattr("niquery.analysis.featuring.requests.get", bad_get)
    with pytest.raises(RuntimeError):
        get_nii_anisotropy_url("http://example/missing.nii.gz")


def test_extract_volume_features(monkeypatch):
    # Prepare input dict: two datasets with small DataFrames
    remote = "openneuro"
    df1 = pd.DataFrame(
        [
            {REMOTE: remote, FULLPATH: "sub-01/func/a_bold.nii.gz"},
            {REMOTE: remote, FULLPATH: "sub-01/func/b_bold.nii.gz"},
        ]
    )
    df2 = pd.DataFrame([{REMOTE: remote, FULLPATH: "sub-02/func/c_bold.nii.gz"}])

    datasets = {"ds1": df1, "ds2": df2}

    timepoints = 50
    voxel_sz = [0.8, 1.0, 1.25]
    anisotropy = 1.5625

    def fake_get_nii_timepoints_s3(bucket, path_str):
        # The implementation under test passes Path(dataset_id) / Path(rec[FULLPATH])
        # so we distinguish by dataset id in the path string.
        if "ds1" in path_str:
            return timepoints
        raise RuntimeError("Pretending that ds2 was not successful")

    def fake_get_nii_voxel_size_s3(bucket, path_str):
        # The implementation under test passes Path(dataset_id) / Path(rec[FULLPATH])
        # so we distinguish by dataset id in the path string.
        if "ds1" in path_str:
            return voxel_sz
        raise RuntimeError("Pretending that ds2 was not successful")

    def fake_get_nii_anisotropy_s3(bucket, path_str):
        # The implementation under test passes Path(dataset_id) / Path(rec[FULLPATH])
        # so we distinguish by dataset id in the path string.
        if "ds1" in path_str:
            return anisotropy
        raise RuntimeError("Pretending that ds2 was not successful")

    monkeypatch.setattr(
        "niquery.analysis.featuring.get_nii_timepoints_s3", fake_get_nii_timepoints_s3
    )
    monkeypatch.setattr(
        "niquery.analysis.featuring.get_nii_voxel_size_s3", fake_get_nii_voxel_size_s3
    )
    monkeypatch.setattr(
        "niquery.analysis.featuring.get_nii_anisotropy_s3", fake_get_nii_anisotropy_s3
    )

    success, failures = extract_volume_features(datasets, max_workers=2)

    voxel_sz_attrs = (VOXEL_SZ,X, VOXEL_SZ_Y, VOXEL_SZ_Z)
    # Success contains ds1 with 2 records and VOLS set, and empty ds2
    assert list(success.keys()) == ["ds1", "ds2"]
    assert [rec[FULLPATH] for rec in success["ds1"]] == sorted(
        [r[FULLPATH] for _, r in df1.iterrows()]
    )
    assert all(rec[VOLS] == timepoints for rec in success["ds1"])
    assert all(np.allclose(rec[idx], voxel_sz[idx]) for rec in success["ds1"] for idx in range(voxel_sz_attrs))
    assert all(np.close(rec[ANISOTROPY], anisotropy) for rec in success["ds1"])
    assert success["ds2"] == []

    # Failures contain ds2's only record
    assert failures == [{DATASETID: "ds2", FULLPATH: df2.iloc[0][FULLPATH], REMOTE: remote}]
