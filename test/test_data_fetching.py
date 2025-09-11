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

import pandas as pd
import pytest

from niquery.data.fetching import fetch_datalad_remote_files
from niquery.data.remotes import DS_TEMPLATE, REMOTES
from niquery.utils.attributes import DATASETID, FULLPATH, REMOTE


@pytest.fixture
def mock_datalad(monkeypatch):
    # Patch the Datalad Dataset API for unit testing without network/side-effects
    class MockDataset:
        created_paths = set()
        cloned_sources = []
        saved_paths = []

        def __init__(self, path):
            self.path = Path(path)
            self._created = False
            self._files = set()

        def create(self, cfg_proc=None):
            MockDataset.created_paths.add(self.path)
            self._created = True

        def clone(self, source, path):
            MockDataset.cloned_sources.append((source, path))
            return MockDataset(path)

        def get(self, fullpath):
            # Simulate that files ending in 'fail.nii.gz' fail, others succeed
            if fullpath.endswith("fail.nii.gz"):
                raise RuntimeError("Failed to get file")
            self._files.add(fullpath)
            return {"status": "ok"}

        def save(self, path):
            MockDataset.saved_paths.append(path)
            return {"status": "saved"}

    monkeypatch.setattr("niquery.data.fetching.Dataset", MockDataset)
    monkeypatch.setattr(
        "niquery.data.fetching.IncompleteResultsError", RuntimeError
    )  # For compatibility
    yield MockDataset


def test_fetch_datalad_remote_files(tmp_path, mock_datalad):
    # Test that a new aggregate dataset is created
    remote = "openneuro"
    df = pd.DataFrame(
        [
            {REMOTE: remote, DATASETID: "ds000001", FULLPATH: "sub-01/file1.nii.gz"},
        ]
    )
    out_dirname = tmp_path
    dataset_name = "ds-agg01"

    mock_datalad.created_paths.clear()

    fetch_datalad_remote_files(df, out_dirname, dataset_name)

    aggr_ds_path = tmp_path / dataset_name
    assert aggr_ds_path in mock_datalad.created_paths

    # Test that subdatasets are cloned and saved
    df = pd.DataFrame(
        [
            {REMOTE: remote, DATASETID: "ds000002", FULLPATH: "sub-02/file2.nii.gz"},
        ]
    )
    out_dirname = tmp_path
    dataset_name = "ds-agg02"

    mock_datalad.cloned_sources.clear()
    mock_datalad.saved_paths.clear()

    fetch_datalad_remote_files(df, out_dirname, dataset_name)

    dataset_id = df.iloc[len(df) - 1][DATASETID]
    ds_url = REMOTES[remote][DS_TEMPLATE].format(DATASET_ID=dataset_id)
    ds_path = tmp_path / dataset_name / dataset_id
    assert any(
        source == ds_url and Path(path) == ds_path for source, path in mock_datalad.cloned_sources
    )
    assert str(ds_path) in mock_datalad.saved_paths

    # Test fetching files and success/failure reporting
    df = pd.DataFrame(
        [
            {REMOTE: remote, DATASETID: "ds000003", FULLPATH: "sub-03/success.nii.gz"},
            {REMOTE: remote, DATASETID: "ds000003", FULLPATH: "sub-03/failure_fail.nii.gz"},
        ]
    )
    out_dirname = tmp_path
    dataset_name = "ds-agg03"

    success, failure = fetch_datalad_remote_files(df, out_dirname, dataset_name)

    assert "ds000003" in success
    assert "sub-03/success.nii.gz" in success["ds000003"]
    assert "ds000003" in failure
    assert "sub-03/failure_fail.nii.gz" in failure["ds000003"]

    # Test that an existing datalad dataset is not re-created
    df = pd.DataFrame(
        [
            {REMOTE: remote, DATASETID: "ds000004", FULLPATH: "sub-04/file.nii.gz"},
        ]
    )
    out_dirname = tmp_path
    dataset_name = "ds-agg04"

    aggr_ds_path = tmp_path / dataset_name
    aggr_ds_path.mkdir(parents=True, exist_ok=False)
    (aggr_ds_path / ".datalad").mkdir()  # Simulate existing datalad dataset

    mock_datalad.created_paths.clear()

    fetch_datalad_remote_files(df, out_dirname, dataset_name)

    # Should not create again
    assert aggr_ds_path not in mock_datalad.created_paths
