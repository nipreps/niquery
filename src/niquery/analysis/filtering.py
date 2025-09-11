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

import ast
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm

from niquery.utils.attributes import (
    DATASETID,
    FILENAME,
    FMRI_MODALITIES,
    HUMAN_SPECIES,
    MODALITIES,
    REMOTE,
    SPECIES,
    VOLS,
)


def filter_nonhuman_datasets(df: pd.DataFrame) -> pd.Series:
    """Filter non-human data records.

    Filters datasets whose 'species' field does not contain one of
    `HUMAN_SPECIES`.

    Parameters
    ----------
    df : :obj:`~pd.DataFrame`
        Dataset records.

    Returns
    -------
    :obj:`~pd.Series`
        Mask of human datasets.
    """

    return df[SPECIES].str.lower().isin(HUMAN_SPECIES)


def filter_nonmri_datasets(df: pd.DataFrame) -> pd.Series:
    """Filter non-MRI data records.

    Filters datasets whose 'modalities' field does not contain one of
    `FMRI_MODALITIES`.

    Parameters
    ----------
    df : :obj:`~pd.DataFrame`
        Dataset records.

    Returns
    -------
    :obj:`~pd.Series`
        Mask of MRI datasets.
    """

    return df[MODALITIES].apply(
        lambda x: any(item.lower() in FMRI_MODALITIES for item in ast.literal_eval(x))
        if isinstance(x, str) and x.startswith("[")
        else False
    )


def filter_nonrelevant_datasets(df: pd.DataFrame) -> pd.DataFrame:
    """Filter non-human and non-MRI data records.

    The 'species' field has to contain 'human' and the 'modalities' field has to
    contain one of :obj:`FMRI_MODALITIES`.

    Parameters
    ----------
    df : :obj:`~pd.DataFrame`
        Dataset records.

    Returns
    -------
    :obj:`~pd.DataFrame`
        Human MRI dataset records.
    """

    species_mask = filter_nonhuman_datasets(df)
    modality_mask = filter_nonmri_datasets(df)

    logging.info(f"Found {sum(~species_mask)}/{len(df)} non-human datasets.")
    logging.info(f"Found {sum(~modality_mask)}/{len(df)} non-MRI datasets.")

    return df[species_mask & modality_mask]


def filter_nonbold_records(fname: str, sep: str) -> pd.DataFrame:
    """Keep records where 'filename' matches BOLD naming.

    Keeps records where 'filename' ends with '_bold.nii.gz'.

    Parameters
    ----------
    fname : :obj:`str`
        Filename.
    sep : :obj:`str`
        Separator.

    Returns
    -------
    :obj:`~pd.DataFrame`
        BOLD file records.
    """

    df = pd.read_csv(fname, sep=sep)
    return df[df[FILENAME].apply(lambda fn: bool(re.search(r"_bold\.nii\.gz$", fn)))]


def identify_bold_files(datasets: dict, sep: str, max_workers: int = 8) -> dict:
    """Identify dataset BOLD files.

    For each dataset, keeps records where 'filename' ends with '_bold.nii.gz'.

    Parameters
    ----------
    datasets : :obj:`dict`
        Dataset file information.
    sep : :obj:`str`
        Separator.
    max_workers : :obj:`int`, optional
        Maximum number of parallel threads to use.

    Returns
    -------
    results : :obj:`dict`
        Dictionary of dataset BOLD files.
    """

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(filter_nonbold_records, val, sep): key for key, val in datasets.items()
        }

        results = {}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Filtering BOLD files"):
            key = futures[future]
            results[key] = future.result()

    return dict(sorted(results.items()))


def filter_on_timepoint_count(
    df: pd.DataFrame, min_timepoints: int, max_timepoints: int
) -> pd.DataFrame:
    """Filter BOLD runs of datasets that are below or above a given number of
    timepoints.

    Filters BOLD runs whose timepoint count is not within the range
    `[min_timepoints, max_timepoints]`.

    Parameters
    ----------
    df : :obj:`~pd.DataFrame`
        BOLD run information.
    min_timepoints : :obj:`int`
        Minimum number of time points.
    max_timepoints : :obj:`int`
        Maximum number of time points.

    Returns
    -------
    :obj:`~pd.DataFrame`
        Filtered BOLD runs.
    """

    # Ensure the BOLD run has [min, max] timepoints (inclusive)
    timepoint_bounds = range(min_timepoints, max_timepoints + 1)
    return df[df[VOLS].isin(timepoint_bounds)]


def filter_on_run_contribution(df: pd.DataFrame, contrib_thr: int, seed: int) -> pd.DataFrame:
    """Filter BOLD runs of datasets to keep their total contribution under a
    threshold.

    Randomly picks BOLD runs of a dataset if the total number of runs exceeds
    the given threshold.

    Parameters
    ----------
    df : :obj:`~pd.DataFrame`
        BOLD run information.
    contrib_thr : :obj:`int`
        Contribution threshold in terms of number of runs.
    seed : :obj:`int`
        Random seed value.

    Returns
    -------
    :obj:`~pd.DataFrame`
        Filtered BOLD runs.
    """

    # Ensure no dataset contributes with more than a given threshold to the
    # total number of runs
    result = (
        df.groupby(DATASETID, group_keys=False)
        .apply(
            lambda x: (
                x.assign(**{DATASETID: x.name}).sample(n=contrib_thr, random_state=seed)
                if len(x) >= contrib_thr
                else x.assign(**{DATASETID: x.name})
            ),
            include_groups=False,
        )  # type: ignore
        .reset_index(drop=True)
    )

    # Make the remote column come first, and the datasetid come second
    return result[
        [REMOTE, DATASETID] + [c for c in result.columns if c not in (REMOTE, DATASETID)]
    ]


def filter_runs(
    df: pd.DataFrame, contrib_thr: int, min_timepoints: int, max_timepoints: int, seed: int
) -> pd.DataFrame:
    """Filter BOLD runs based on run count and timepoint criteria.

    Filters the BOLD runs to include only those that fulfil:
      - Criterion 1: the number of runs for a given dataset is below the
        threshold `contrib_thr`.
      - Criterion 2: the number of timepoints per BOLD run is between
       `[min_timepoints, max_timepoints]`.

    Parameters
    ----------
    df : :obj:`~pd.DataFrame`
        BOLD run information.
    contrib_thr : :obj:`int`
        Contribution threshold in terms of number of runs.
    min_timepoints : :obj:`int`
        Minimum number of time points.
    max_timepoints : :obj:`int``
        Maximum number of time points.
    seed : :obj:`int`
        Random seed value.

    Returns
    -------
    :obj:`~pd.DataFrame`
        Filtered BOLD runs.
    """

    # Criterion 2: the BOLD run has [min, max] timepoints (inclusive)
    df = filter_on_timepoint_count(df, min_timepoints, max_timepoints)

    # Criterion 1: the number of runs for a given dataset is below a threshold
    df = filter_on_run_contribution(df, contrib_thr, seed)

    return df


def identify_relevant_runs(
    df: pd.DataFrame,
    contrib_thr: int,
    min_timepoints: int,
    max_timepoints: int,
    seed: int,
) -> pd.DataFrame:
    """Identify relevant BOLD runs in terms of run and timepoint count constraints.

    Identifies the BOLD runs that fulfill the following criteria:
      - Criterion 1: the number of runs for a given dataset is below the
        threshold `contrib_thr`.
      - Criterion 2: the number of timepoints per BOLD run is between
       `[min_timepoints, max_timepoints]`.

    Runs are shuffled before the filtering process.

    Parameters
    ----------
    df : :obj:`~pd.DataFrame`
        BOLD run information.
    contrib_thr : :obj:`int`
        Contribution threshold in terms of the number of runs a dataset can
        contribute with over the total number of runs.
    min_timepoints : :obj:`int`
        Minimum number of time points.
    max_timepoints : :obj:`int``
        Maximum number of time points.
    seed : :obj:`int`
        Random seed value.

    Returns
    -------
    :obj:`~pd.DataFrame`
        Identified relevant BOLD runs.
    """

    # Shuffle records for randomness
    df = df.sample(frac=1, random_state=seed)

    # Filter runs
    df = filter_runs(df, contrib_thr, min_timepoints, max_timepoints, seed)

    return df
