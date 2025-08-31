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

import numpy as np
import pandas as pd

from niquery.io.utils import (
    append_modality_to_filename,
    write_dataset_file_lists,
    write_dataset_paths,
    write_dataset_tags,
)
from niquery.utils.attributes import DATASETID, FILENAME, FULLPATH, MODALITIES, TAG

DSV_SEPARATOR = "\t"


def test_append_modality_to_filename():
    fname = Path("openneuro_datasets.tsv")
    modality = "mri"
    new_fname = append_modality_to_filename(fname, modality)
    assert new_fname.name == "openneuro_datasets_mri.tsv"
    assert isinstance(new_fname, Path)

    fname = Path("penneuro_datasets")
    modality = "pet"
    new_fname = append_modality_to_filename(fname, modality)
    assert new_fname.name == "penneuro_datasets_pet"


def test_write_dataset_file_lists_creates_tsv(tmp_path):
    file_dict = {
        "ds1": [
            {FILENAME: "a.nii.gz", MODALITIES: ["mri"]},
            {FILENAME: "b.nii.gz", MODALITIES: ["eeg"]},
        ],
        "ds2": [
            {FILENAME: "c.nii.gz", MODALITIES: ["pet"]},
        ],
        "empty_ds": [],
    }
    sep = DSV_SEPARATOR
    dirname = tmp_path
    write_dataset_file_lists(file_dict, dirname, sep)
    ds1_path = dirname / "ds1.tsv"
    ds2_path = dirname / "ds2.tsv"
    empty_ds_path = dirname / "empty_ds.tsv"
    # Both ds1 and ds2 should exist, empty_ds should not.
    assert ds1_path.exists()
    assert ds2_path.exists()
    assert not empty_ds_path.exists()

    # Check contents
    df1 = pd.read_csv(ds1_path, sep=sep)
    assert set(df1.columns) == {FILENAME, MODALITIES}
    assert df1.shape == (2, 2)
    assert (df1[FILENAME] == ["a.nii.gz", "b.nii.gz"]).all()

    df2 = pd.read_csv(ds2_path, sep=sep)
    assert df2.shape == (1, 2)
    assert df2[FILENAME].iloc[0] == "c.nii.gz"
    assert df2[MODALITIES].iloc[0] == "['pet']"


def test_write_dataset_file_lists_fillna(tmp_path):
    file_dict = {
        "ds1": [
            {FILENAME: "a.nii.gz", MODALITIES: None},
            {FILENAME: None, MODALITIES: ["bold"]},
        ],
    }
    expected_values = np.asarray([["a.nii.gz", np.nan], [np.nan, "['bold']"]], dtype=object)
    expected_df = pd.DataFrame(expected_values, columns=[FILENAME, MODALITIES])
    sep = DSV_SEPARATOR
    dirname = tmp_path
    write_dataset_file_lists(file_dict, dirname, sep)
    ds1_path = dirname / "ds1.tsv"
    df = pd.read_csv(ds1_path, sep=sep)
    pd.testing.assert_frame_equal(df, expected_df)


def test_write_dataset_paths(tmp_path):
    paths = [
        {DATASETID: "ds1", FULLPATH: "sub-01/func/sub-01_task-rest_echo-1_bold.nii.gz"},
        {
            DATASETID: "ds2",
            FULLPATH: "derivatives/sub-101/ses-1/func/sub-RC4101_ses-1_task-ANT_run-1_bold.nii.gz",
        },
    ]
    sep = DSV_SEPARATOR
    fname_paths = tmp_path / "paths.tsv"
    write_dataset_paths(paths, fname_paths, sep)

    df_paths = pd.read_csv(fname_paths, sep=sep)
    assert set(df_paths.columns) == {DATASETID, FULLPATH}
    assert df_paths.shape == (2, 2)
    assert (df_paths[DATASETID] == ["ds1", "ds2"]).all()


def test_write_dataset_tags(tmp_path):
    tags = [
        {DATASETID: "ds1", TAG: "1.0.0"},
        {DATASETID: "ds2", TAG: "2.0.2"},
    ]
    sep = DSV_SEPARATOR
    fname_tags = tmp_path / "tags.tsv"
    write_dataset_tags(tags, fname_tags, sep)

    df_tags = pd.read_csv(fname_tags, sep=sep)
    assert set(df_tags.columns) == {DATASETID, TAG}
    assert df_tags.shape == (2, 2)
    assert (df_tags[TAG] == ["1.0.0", "2.0.2"]).all()
