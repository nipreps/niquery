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

from datalad.api import Dataset  # type: ignore[import-untyped]
from datalad.support.exceptions import IncompleteResultsError  # type: ignore[import-untyped]

from niquery.data.remotes import DS_TEMPLATE, REMOTES
from niquery.utils.attributes import DATASETID, FULLPATH, REMOTE
from niquery.utils.decorators import require_datalad


@require_datalad
def fetch_datalad_remote_files(df, out_dirname, dataset_name) -> tuple:
    """Fetch files from remote DataLad datasets.

    Downloads only the files listed in the provided DataFrame instance. The
    DataFrame is expected to contain at least the following columns:

      - ``remote``: Remote server name (e.g., 'openneuro')
      - ``datasetid``: Dataset identifier (e.g., 'ds000231')
      - ``fullpath``: Path of the file within the dataset (e.g.
        'sub-01/func/sub-01_task-flavor_run-02_bold.nii.gz')

    If the DataLad dataset already exists in the provided path, it is not
    cloned again.

    A new DataLad dataset is created at the destination path, and each dataset
    is made to be a subdataset.

    Parameters
    ----------
    df : :obj:`~pandas.DataFrame`
        Table containing at least 'remote', 'datasetid', and 'fullpath' columns.
        Each row corresponds to a file to be fetched.
    out_dirname : :obj:`~pathlib.Path`
        Output directory where the datasets will be cloned and files stored.
    dataset_name : :obj:`str`
        Name of the dataset.

    Returns
    -------
    fetched_files, failure_results : :obj:`tuple`
        Dictionary of datasets and the filenames succeeded/failed for each.
    """

    # Group by remote, dataset_id; collect all file paths for each dataset
    grouped = df.groupby([REMOTE, DATASETID])

    # Create new datalad dataset
    aggr_ds_path = out_dirname / dataset_name
    aggr_ds_path.mkdir(parents=True, exist_ok=True)
    if not (aggr_ds_path / ".datalad").exists():
        aggr_ds = Dataset(str(aggr_ds_path))
        aggr_ds.create(cfg_proc="text2git")
    else:
        aggr_ds = Dataset(str(aggr_ds_path))

    success_results: dict[str, list[str]] = {}
    failure_results: dict[str, list[str]] = {}

    # Loop over remote, dataset pairs
    for (remote, dataset_id), file_list in grouped:
        ds_url = REMOTES[remote][DS_TEMPLATE].format(DATASET_ID=dataset_id)
        ds_path = aggr_ds_path / str(dataset_id)
        if not ds_path.exists():
            ds = aggr_ds.clone(source=ds_url, path=str(ds_path))
        else:
            # If already cloned, ensure it's a datalad dataset
            ds = Dataset(str(ds_path))
            assert (ds_path / ".datalad").exists()

        # Now, get each relevant file in this dataset
        for _, file in file_list.iterrows():
            fullpath = str(file[FULLPATH])
            try:
                _ = ds.get(fullpath)
                success_results.setdefault(dataset_id, []).append(fullpath)
            except (RuntimeError, IncompleteResultsError) as _:
                failure_results.setdefault(dataset_id, []).append(fullpath)

        aggr_ds.save(path=str(ds_path))

    return success_results, failure_results
