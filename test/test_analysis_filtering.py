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

import pandas as pd

from niquery.analysis.filtering import (
    filter_nonbold_records,
    filter_nonhuman_datasets,
    filter_nonmri_datasets,
    filter_nonrelevant_datasets,
    filter_on_run_contribution,
    filter_on_timepoint_count,
    filter_runs,
    identify_bold_files,
    identify_relevant_runs,
)
from niquery.utils.attributes import DATASETID, FILENAME, MODALITIES, SPECIES, VOLS

DSV_SEPARATOR = "\t"


def test_filter_nonhuman_datasets():
    df = pd.DataFrame(
        [
            {SPECIES: "human"},
            {SPECIES: "mouse"},
            {SPECIES: "Human"},
        ]
    )

    species_mask = filter_nonhuman_datasets(df)
    assert species_mask.tolist() == [True, False, True]


def test_filter_nonmri_datasets():
    df = pd.DataFrame(
        [
            {MODALITIES: "['fMRI']"},
            {MODALITIES: "['eeg']"},
            {MODALITIES: "[]"},
        ]
    )

    modality_mask = filter_nonmri_datasets(df)
    assert modality_mask.tolist() == [True, False, False]


def test_filter_nonrelevant_datasets():
    df = pd.DataFrame(
        [
            {SPECIES: "human", MODALITIES: "['fMRI']"},
            {SPECIES: "mouse", MODALITIES: "['fMRI']"},  # species fails
            {SPECIES: "human", MODALITIES: "['eeg']"},  # modality fails
        ]
    )
    out = filter_nonrelevant_datasets(df)
    # Only the first row survives
    assert len(out) == 1
    assert out.iloc[0][SPECIES].lower() == "human"
    assert out.iloc[0][MODALITIES].lower() == "['fmri']"


def test_filter_nonbold_records(tmp_path):
    csv = tmp_path / "files.tsv"
    rows = [
        {FILENAME: "sub-01_task-rest_bold.nii.gz"},
        {FILENAME: "sub-02_task-rest_T1w.nii.gz"},
        {FILENAME: "sub-03_task-rest_bold.nii.gz"},
        {FILENAME: "sub-04_task-rest_bold.nii"},  # missing .gz
    ]
    pd.DataFrame(rows).to_csv(csv, sep=DSV_SEPARATOR, index=False)

    df = filter_nonbold_records(str(csv), sep=DSV_SEPARATOR)
    assert df[FILENAME].tolist() == [
        "sub-01_task-rest_bold.nii.gz",
        "sub-03_task-rest_bold.nii.gz",
    ]


def test_identify_bold_files(tmp_path):
    # Create two TSV files
    f1 = tmp_path / "a.tsv"
    f2 = tmp_path / "b.tsv"
    pd.DataFrame([{FILENAME: "x_bold.nii.gz"}, {FILENAME: "y_T1w.nii.gz"}]).to_csv(
        f1, sep=DSV_SEPARATOR, index=False
    )
    pd.DataFrame([{FILENAME: "z_bold.nii.gz"}]).to_csv(f2, sep=DSV_SEPARATOR, index=False)

    datasets = {"ds1": str(f1), "ds2": str(f2)}
    out = identify_bold_files(datasets, sep=DSV_SEPARATOR)

    # Keys sorted
    assert list(out.keys()) == ["ds1", "ds2"]
    assert out["ds1"][FILENAME].tolist() == ["x_bold.nii.gz"]
    assert out["ds2"][FILENAME].tolist() == ["z_bold.nii.gz"]


def test_filter_on_timepoint_count():
    df = pd.DataFrame([{VOLS: 100}, {VOLS: 200}, {VOLS: 300}])
    out = filter_on_timepoint_count(df, min_timepoints=150, max_timepoints=300)
    assert out[VOLS].tolist() == [200, 300]


def test_filter_on_run_contribution_sampling_and_column_order():
    df = pd.DataFrame(
        [
            {DATASETID: "ds1", VOLS: 100},
            {DATASETID: "ds1", VOLS: 200},
            {DATASETID: "ds1", VOLS: 300},
            {DATASETID: "ds2", VOLS: 150},
        ]
    )
    out = filter_on_run_contribution(df, contrib_thr=2, seed=1234)

    # ds1 limited to 2 rows, ds2 unchanged -> total 3
    assert len(out) == 3
    # Only ds1 rows were potentially sampled; all rows are from ds1 or ds2
    assert set(out[DATASETID]) <= {"ds1", "ds2"}
    # ds2 appears exactly once
    assert (out[DATASETID] == "ds2").sum() == 1


def test_filter_runs():
    df = pd.DataFrame(
        [
            {DATASETID: "ds1", VOLS: 100},
            {DATASETID: "ds1", VOLS: 200},
            {DATASETID: "ds1", VOLS: 300},
            {DATASETID: "ds2", VOLS: 150},
        ]
    )
    out = filter_runs(df, contrib_thr=2, min_timepoints=150, max_timepoints=300, seed=1234)
    # Timepoint filtering leaves 200,300,150, then contribution thr limits ds1 to 2 rows total
    assert len(out) == 3
    assert set(out[VOLS]) <= {150, 200, 300}


def test_identify_relevant_runs():
    df = pd.DataFrame(
        [
            {DATASETID: "ds1", VOLS: 100},
            {DATASETID: "ds1", VOLS: 200},
            {DATASETID: "ds1", VOLS: 300},
            {DATASETID: "ds2", VOLS: 150},
        ]
    )
    out = identify_relevant_runs(
        df, contrib_thr=2, min_timepoints=150, max_timepoints=300, seed=1234
    )
    # Same expectations as filter_runs; ordering may differ due to shuffling
    assert len(out) == 3
    assert set(out[VOLS]) == {150, 200, 300}
