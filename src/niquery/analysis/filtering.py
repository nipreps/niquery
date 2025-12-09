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
    MODALITIES,
    REMOTE,
    SPECIES,
    VOLS,
)


def filter_species_datasets(df: pd.DataFrame, species: str | list) -> pd.Series:
    """Filter non-relevant species data records.

    Filters datasets whose 'species' field does not contain one of items in
    ``species``.

    Parameters
    ----------
    df : :obj:`~pandas.DataFrame`
        Dataset records.
    species : :obj:`str` or :obj:`list`
        Species to consider (case-insensitive).

    Returns
    -------
    :obj:`~pandas.Series`
        Mask of relevant datasets.
    """

    if isinstance(species, str):
        species = [species]

    return df[SPECIES].str.lower().isin(species)


def filter_modality_datasets(df: pd.DataFrame, modality: str | list) -> pd.Series:
    """Filter non-relevant modality data records.

    Filters datasets whose 'modalities' field does not contain one of items in
    ``modality``.

    Parameters
    ----------
    df : :obj:`~pandas.DataFrame`
        Dataset records.
    modality : :obj:`str` or :obj:`list`
        Modalities to consider (case-insensitive).

    Returns
    -------
    :obj:`~pandas.Series`
        Mask of relevant datasets.
    """

    if isinstance(modality, str):
        modality = [modality]

    return df[MODALITIES].apply(
        lambda x: any(item.lower() in modality for item in ast.literal_eval(x))
        if isinstance(x, str) and x.startswith("[")
        else False
    )


def filter_nonrelevant_datasets(
    df: pd.DataFrame, species: str | list, modality: str | list
) -> pd.DataFrame:
    """Filter non-relevant data records.

    Return datasets that belong to the provided species and modality..

    Parameters
    ----------
    df : :obj:`~pandas.DataFrame`
        Dataset records.
    species : :obj:`str` or :obj:`list`
        Species to consider (case-insensitive).
    modality : :obj:`str` or :obj:`list`
        Modalities to consider (case-insensitive).

    Returns
    -------
    :obj:`~pandas.DataFrame`
        Relevant dataset records.

    See Also
    --------
    :obj:`~niquery.analysis.filtering.filter_modality_datasets`
    :obj:`~niquery.analysis.filtering.filter_modality_datasets`
    """

    species_mask = filter_species_datasets(df, species)
    modality_mask = filter_modality_datasets(df, modality)

    logging.info(f"Found {sum(~species_mask)}/{len(df)} datasets from other species.")
    logging.info(f"Found {sum(~modality_mask)}/{len(df)} datasets from other modalities.")

    return df[species_mask & modality_mask]


def filter_modality_records(fname: str, sep: str, suffix: str | list) -> pd.DataFrame:
    """Keep records where the filename matches the provided modality naming convention.

    Following the
    `BIDS modality suffix convention <https://bids.neuroimaging.io/getting_started/folders_and_files/files.html#filename-template>`__,
    keeps records where the 'filename' attribute ends with the given suffix,
    i.e. '_{suffix}.nii.gz'.

    Parameters
    ----------
    fname : :obj:`str`
        Filename. A delimiter-separated file containing the list of records to
        be inspected.
    sep : :obj:`str`
        Separator.
    suffix : :obj:`str` or :obj:`list`
        Suffix of the relevant files.

    Returns
    -------
    :obj:`~pandas.DataFrame`
        Modality file records.
    """

    if isinstance(suffix, str):
        suffix = [suffix]

    pattern = "(" + "|".join([s + ".nii.gz" for s in suffix]) + ")"
    df = pd.read_csv(fname, sep=sep)
    return df[df[FILENAME].apply(lambda fn: bool(re.search(pattern, fn)))]


def identify_modality_files(
    datasets: dict, sep: str, suffix: str | list, max_workers: int = 8
) -> dict:
    """Identify dataset files having a particular suffix.

    For each dataset, and following the
    `BIDS modality suffix convention <https://bids.neuroimaging.io/getting_started/folders_and_files/files.html#filename-template>`__,
    keeps records where the 'filename' attribute ends with '_{suffix}.nii.gz'.

    Parameters
    ----------
    datasets : :obj:`dict`
        Dataset file information. Contains a list of datasets ids and the
        corresponding delimiter-separated files containing the list of records
        to be inspected.
    suffix : :obj:`str` or :obj:`list`
        Suffix of the relevant files.
    sep : :obj:`str`
        Separator.
    max_workers : :obj:`int`, optional
        Maximum number of parallel threads to use.

    Returns
    -------
    results : :obj:`dict`
        Dictionary of dataset modality-specific file records.

    See Also
    --------
    :obj:`~niquery.analysis.filtering.filter_modality_records`
    """

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(filter_modality_records, val, sep, suffix): key
            for key, val in datasets.items()
        }

        results = {}
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Filtering modality files"
        ):
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
    df : :obj:`~pandas.DataFrame`
        BOLD run information.
    min_timepoints : :obj:`int`
        Minimum number of time points.
    max_timepoints : :obj:`int`
        Maximum number of time points.

    Returns
    -------
    :obj:`~pandas.DataFrame`
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
    df : :obj:`~pandas.DataFrame`
        BOLD run information.
    contrib_thr : :obj:`int`
        Contribution threshold in terms of number of runs.
    seed : :obj:`int`
        Random seed value.

    Returns
    -------
    :obj:`~pandas.DataFrame`
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
    df : :obj:`~pandas.DataFrame`
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
    :obj:`~pandas.DataFrame`
        Filtered BOLD runs.

    See Also
    --------
    :obj:`~niquery.analysis.filtering.filter_on_timepoint_count`
    :obj:`~niquery.analysis.filtering.filter_on_run_contribution`
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
    df : :obj:`~pandas.DataFrame`
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
    :obj:`~pandas.DataFrame`
        Identified relevant BOLD runs.

    See Also
    --------
    :obj:`~niquery.analysis.filtering.filter_runs`
    """

    # Shuffle records for randomness
    df = df.sample(frac=1, random_state=seed)

    # Filter runs
    df = filter_runs(df, contrib_thr, min_timepoints, max_timepoints, seed)

    return df
