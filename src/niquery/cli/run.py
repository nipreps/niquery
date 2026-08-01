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
"""
NiQuery runner for characterizing and selecting OpenNeuro BOLD runs.
"""

import logging
import os
import sys
import time
from pathlib import Path

import click
import pandas as pd

from niquery.analysis.featuring import extract_volume_features
from niquery.analysis.filtering import (
    filter_nonrelevant_datasets,
    identify_modality_files,
    identify_relevant_runs,
)
from niquery.cli.utils import force_output, verify_output_path
from niquery.data.fetching import fetch_datalad_remote_files
from niquery.data.utils import filter_non_conforming_ds
from niquery.io.utils import (
    append_label_to_filename,
    write_dataset_file_lists,
    write_dataset_paths,
    write_dataset_tags,
)
from niquery.query.querying import (
    edges_to_dataframe,
    fetch_pages,
    get_cursors,
    query_datasets,
)
from niquery.utils.attributes import DATASETID, FILENAME, TAG
from niquery.utils.decorators import require_datalad_click
from niquery.utils.logging import configure_logging

DSV_SEPARATOR = "\t"

CONTR_FRACTION = 0.05
"""Allowed contribution fraction for runs per dataset over the total runs."""

MIN_TIMEPOINTS = 300
"""Minimum number of BOLD timepoints per dataset."""

MAX_TIMEPOINTS = 1200
"""Maximum number BOLD timepoints per dataset."""

TOTAL_RUNS = 4000
"""Number of total runs."""

MAX_WORKERS = min(32, os.cpu_count() or 1)
"""Maximum number of workers to use for parallel processing. Cap at 32 to \
prevent overcommitting in high-core systems."""


@click.group()
def cli() -> None:
    """CLI for working with OpenNeuro BOLD runs."""
    pass


@cli.command()
@click.argument("remote", type=click.STRING)
@click.argument("out_filename", type=click.Path(dir_okay=False, path_type=Path))
@force_output
def index(remote, out_filename, force) -> None:  ## Stores tasks (specific to fMRI) (open issue)
    """Index existing dataset information from a remote server.

    The remote server needs to offer a GraphQL API.

    The 'remote', 'id', 'name', 'species', 'tag', 'dataset_doi', 'modalities',
    and 'tasks' features of the available datasets are stored in the output file
    using a delimiter-separated format.

    REMOTE       str    Remote server name
    OUT_FILENAME path   Output filename
    """

    verify_output_path(out_filename, force)

    configure_logging(out_filename.parent, sys._getframe().f_code.co_name)

    logging.info(
        "Script called with arguments:\n" + "\n".join(f"  {k}: {v}" for k, v in locals().items())
    )

    logging.info(f"Indexing {remote}...")

    start = time.time()

    # Precompute all cursors
    cursors = get_cursors(remote)

    # Fetch all pages in parallel
    edges = fetch_pages(cursors, max_workers=MAX_WORKERS)

    end = time.time()
    duration = end - start

    logging.info(f"Found {len(edges)} datasets in {duration:.2f} seconds.")

    # Serialize
    df = edges_to_dataframe(edges)
    df.to_csv(out_filename, sep=DSV_SEPARATOR, index=False)


@cli.command()
@click.argument(
    "in_filename",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.argument("out_dirname", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--species",
    multiple=True,
    required=True,
    help="Species to consider",
    type=str,
)
@click.option(
    "--modality",
    multiple=True,
    required=True,
    help="Modalities to consider",
    type=str,
)
@force_output
def collect(in_filename, out_dirname, species, modality, force) -> None:
    """Collect datasets' file information using the remotes and IDs read from
    the input file.

    For each queried dataset, the list of files is stored in a
    delimiter-separated file, along with the 'remote', 'id', 'filename', 'size',
    'directory', 'annexed', 'key', 'urls', and 'fullpath' features.

    IN_FILENAME path  Dataset list filename

    OUT_DIRNAME path  Output dirname

    MODALITY    str   Modalities to consider

    SPECIES     str   Species to consider
    """

    verify_output_path(out_dirname, force)

    configure_logging(out_dirname, sys._getframe().f_code.co_name)

    logging.info(
        "Script called with arguments:\n" + "\n".join(f"  {k}: {v}" for k, v in locals().items())
    )

    sep = DSV_SEPARATOR

    start = time.time()

    # Ensure that the tag column is read as a string to prevent leading zeros
    # from being stripped. Similarly, keep the "NA" values empty as otherwise
    # pandas loads them as "NaN" which is not considered a string but a number
    # and causes issues downstream when trying to compare values for sorting
    # results.
    _df = pd.read_csv(
        in_filename, sep=sep, dtype={TAG: str}, keep_default_na=False, na_values=[""]
    )

    logging.info(f"Filtering {len(_df)} datasets...")

    # Filter nonrelevant datasets
    df = filter_nonrelevant_datasets(_df, species, modality)

    logging.info(
        f"Filtered {len(_df) - len(df)}/{len(_df)} non-{species}, non-{modality} datasets."
    )

    fname = append_label_to_filename(in_filename, "relevant")
    datasets_fname = Path.joinpath(out_dirname, fname)
    df.to_csv(datasets_fname, sep=sep, index=False)

    success_results, failure_results = query_datasets(df, max_workers=MAX_WORKERS)

    end = time.time()
    duration = end - start

    # Compute success/failure ratios
    success_count = len(success_results)
    failure_count = len(failure_results)
    total_count = success_count + failure_count

    success_ratio = success_count / total_count * 100 if total_count else 0
    failure_ratio = failure_count / total_count * 100 if total_count else 0

    logging.info(f"Collected {total_count} datasets in {duration:.2f} seconds.")
    logging.info(f"{success_count}/{total_count} queries succeeded ({success_ratio:.2f}%).")
    logging.info(f"{failure_count}/{total_count} queries failed ({failure_ratio:.2f}%).")

    # Serialize
    write_dataset_file_lists(success_results, out_dirname, sep)
    failed_datasets_info_fname = Path.joinpath(out_dirname, "failed_dataset_tag_queries.tsv")
    write_dataset_tags(failure_results, failed_datasets_info_fname, sep)


@cli.command()
@click.argument("in_dirname", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("out_dirname", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--suffix",
    multiple=True,
    required=True,
    help="Modality suffixes to consider",
    type=str,
)
@force_output
def analyze(in_dirname, out_dirname, suffix, force) -> None:
    """Analyze modality-specific files in datasets to extract relevant features.

    Analyzes the modality-specific data files contained in the records of each
    dataset in the input directory. The ``suffix`` indicates what files will be
    analyzed. The features computed include the number of volumes of each file.

    IN_DIRNAME path   Input dirname

    OUT_DIRNAME path  Output dirname

    SUFFIX      str   Modality suffix to consider
    """

    verify_output_path(out_dirname, force)

    configure_logging(out_dirname, sys._getframe().f_code.co_name)

    logging.info(
        "Script called with arguments:\n" + "\n".join(f"  {k}: {v}" for k, v in locals().items())
    )

    sep = DSV_SEPARATOR

    start = time.time()

    # Consider only files that have the "ds\d{6}\.tsv" pattern (e.g.
    # ds000006.tsv, ds000021.tsv, etc.)
    datasets = filter_non_conforming_ds(in_dirname)

    ds_count = len(datasets)
    logging.info(f"Characterizing {ds_count} datasets...")

    files = identify_modality_files(datasets, sep, suffix, max_workers=MAX_WORKERS)

    file_count = sum(len(item) for item in files.values())
    logging.info(f"Found {file_count} relevant files.")

    success_results, failure_results = extract_volume_features(files)

    end = time.time()
    duration = end - start

    # Compute success/failure ratios
    success_ds = len(success_results)
    failure_ds = len({item[DATASETID] for item in failure_results})

    success_files = sum(len(v) for v in success_results.values())
    failure_files = len(failure_results)

    success_ds_ratio = success_ds / ds_count * 100 if ds_count else 0
    failure_ds_ratio = failure_ds / ds_count * 100 if ds_count else 0

    success_file_ratio = success_files / file_count * 100 if file_count else 0
    failure_file_ratio = failure_files / file_count * 100 if file_count else 0

    logging.info(f"Characterized {file_count} BOLD runs in {duration:.2f} seconds.")
    logging.info(
        f"{success_files}/{file_count} analyses succeeded ({success_file_ratio:.2f}%) "
        f"from {success_ds}/{ds_count} ({success_ds_ratio:.2f}%) datasets."
    )
    logging.info(
        f"{failure_files}/{file_count} analyses failed ({failure_file_ratio:.2f}%) "
        f"from {failure_ds}/{ds_count} datasets ({failure_ds_ratio:.2f}%)."
    )

    write_dataset_file_lists(success_results, out_dirname, sep)
    failed_files = Path(out_dirname, "failed_dataset_tag_queries.tsv")
    write_dataset_paths(failure_results, failed_files, sep)


@cli.command()
@click.argument("in_dirname", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("out_filename", type=click.Path(file_okay=True, path_type=Path))
@click.argument("seed", type=int)
@click.option(
    "--total-runs", type=int, default=TOTAL_RUNS, show_default=True, help="Number of total runs"
)
@click.option(
    "--contr-fraction",
    type=float,
    default=CONTR_FRACTION,
    show_default=True,
    help="Allowed contribution fraction for runs per dataset over the total runs",
)
@click.option(
    "--min-timepoints",
    type=int,
    default=MIN_TIMEPOINTS,
    show_default=True,
    help="Minimum number of BOLD timepoints per dataset",
)
@click.option(
    "--max-timepoints",
    type=int,
    default=MAX_TIMEPOINTS,
    show_default=True,
    help="Maximum number BOLD timepoints per dataset",
)
@force_output
def select(
    in_dirname,
    out_filename,
    seed,
    total_runs,
    contr_fraction,
    min_timepoints,
    max_timepoints,  # These will also need to have aNone default value to generalize
    force,
) -> None:  ## Generalize name or approach to allow other modalities (open issue)
    """Select relevant BOLD runs based on constraints.

    Selects relevant fMRI BOLD runs based on the following set of constraints:

      - Criterion 1: no single dataset shall contribute more than a fraction of
        the total number of runs.

      - Criterion 2: each BOLD run shall have between a minimum and maximum
        number of timepoints (inclusive).

    Results are stored using a delimiter-separated format.

    IN_DIRNAME path    Input dirname

    OUT_FILENAME path  Output data list filename

    SEED integer       Random seed. Use the format 'YYYYMMDD' for a date
    """

    verify_output_path(out_filename, force)

    configure_logging(out_filename.parent, sys._getframe().f_code.co_name)

    logging.info(
        "Script called with arguments:\n" + "\n".join(f"  {k}: {v}" for k, v in locals().items())
    )

    sep = DSV_SEPARATOR

    start = time.time()

    # Consider only files that have the "ds\d{6}\.tsv" pattern (e.g.
    # ds000006.tsv, ds000021.tsv, etc.)
    datasets = filter_non_conforming_ds(in_dirname)

    df = pd.concat([pd.read_csv(val, sep=sep) for val in datasets.values()], ignore_index=True)

    run_count = len(df)
    logging.info(f"Analyzing {run_count} runs...")

    # Identify runs fulfilling the criteria
    contrib_thr = int(contr_fraction * total_runs)
    df_rel_runs = identify_relevant_runs(
        df,
        contrib_thr,
        min_timepoints,
        max_timepoints,
        seed,
    )

    end = time.time()
    duration = end - start

    rel_run_count = len(df_rel_runs)
    rel_ratio = rel_run_count / run_count * 100 if run_count else 0

    logging.info(
        f"Identified {rel_run_count}/{run_count} relevant runs ({rel_ratio:.2f}%) "
        f"in {duration:.2f} seconds."
    )

    # Keep only the first `total_runs`
    df_sel_runs = df_rel_runs.head(total_runs).sort_values(by=[DATASETID, FILENAME])

    sel_run_count = len(df_sel_runs)
    sel_ratio = sel_run_count / rel_run_count * 100 if rel_run_count else 0

    logging.info(f"Selected the first {sel_run_count}/{rel_run_count} ({sel_ratio:.2f}%) runs.")

    df_sel_runs.fillna("NA", inplace=True)
    df_sel_runs.to_csv(out_filename, sep=sep, index=False)


@cli.command()
@require_datalad_click
@click.argument(
    "in_filename",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.argument("out_dirname", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("name")
@force_output
def aggregate(in_filename, out_dirname, name, force) -> None:
    """Aggregate NIfTI files listed in the input file into a new DataLad
    dataset.

    Clones the corresponding OpenNeuro datasets, and downloads only those files
    listed towards the creation of the new dataset.

    IN_FILENAME path  NIfTI filename list filename

    OUT_DIRNAME path  Output dirname

    NAME        str   Name of the dataset
    """

    verify_output_path(out_dirname, force)

    configure_logging(out_dirname, sys._getframe().f_code.co_name)

    logging.info(
        "Script called with arguments:\n" + "\n".join(f"  {k}: {v}" for k, v in locals().items())
    )

    # Read selected dataset IDs
    df = pd.read_csv(in_filename, sep=DSV_SEPARATOR)

    success_results, failure_results = fetch_datalad_remote_files(df, out_dirname, name)

    # Compute success/failure ratios
    success_ds = len(success_results)
    failure_ds = len(failure_results)
    ds_count = success_ds + failure_ds

    success_files = sum(len(v) for v in success_results.values())
    failure_files = sum(len(v) for v in failure_results.values())
    file_count = success_files + failure_files

    success_ds_ratio = success_ds / ds_count * 100 if ds_count else 0
    failure_ds_ratio = failure_ds / ds_count * 100 if ds_count else 0

    success_files_ratio = success_files / file_count * 100 if file_count else 0
    failure_files_ratio = failure_files / file_count * 100 if file_count else 0

    logging.info(
        f"Failures reported for {failure_files}/{file_count} ({failure_files_ratio:.2f}%) files "
        f"from {failure_ds}/{ds_count} datasets ({failure_ds_ratio:.2f}%)."
    )
    logging.info(
        f"Aggregated {success_files} ({success_files_ratio:.2f}%) files "
        f"from {success_ds}/{ds_count} datasets ({success_ds_ratio:.2f}%) into {out_dirname}."
    )


if __name__ == "__main__":
    cli()
