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


def append_label_to_filename(in_filename: Path, label: str) -> Path:
    """Compose a new path by appending the label tag to the file rootname.

    Parameters
    ----------
    in_filename : :obj:`~pathlib.Path`
        Filename.
    label : :obj:`str`
        Label tag.

    Returns
    -------
    :obj:`~pathlib.Path`
        Composed filename.
    """

    return in_filename.with_name(in_filename.stem + f"_{label}" + in_filename.suffix)


def write_dataset_file_lists(file_dict: dict, dirname: Path, sep: str) -> None:
    """Write each dataset's list of files to a TSV file.

    Writes each file list as a TSV named <dataset_id>.tsv, and uses dict keys as
    columns. Skips entries with empty lists.

    Parameters
    ----------
    file_dict: :obj:`dict`
        A mapping from dataset ID to a list of file metadata dicts.
    dirname : :obj:`~pathlib.Path`
        Directory where TSV files will be written.
    sep : :obj:`str`
        Separator.
    """

    for dataset_id, file_list in file_dict.items():
        if not file_list:
            continue

        df = pd.DataFrame(file_list)
        df.fillna("NA", inplace=True)
        fname = Path.joinpath(dirname, f"{dataset_id}.tsv")
        df.to_csv(fname, sep=sep, index=False)


def write_dataset_paths(dataset_paths: list, fname: Path, sep: str) -> None:
    """Write dataset tag dictionaries to a TSV file.

    Parameters
    ----------
    dataset_paths : :obj:`list`
        Dictionaries of dataset ID and fullpath.
    fname : :obj:`~pathlib.Path`
        Filename.
    sep : :obj:`str`
        Separator.
    """

    df = pd.DataFrame(dataset_paths)
    df.to_csv(fname, sep=sep, index=False)


def write_dataset_tags(dataset_tags: list, fname: Path, sep: str) -> None:
    """Write dataset tag dictionaries to a TSV file.

    Parameters
    ----------
    dataset_tags : :obj:`list`
        Dictionaries of dataset ID and snapshot tags.
    fname : :obj:`~pathlib.Path`
        Filename.
    sep : :obj:`str`
        Separator.
    """

    df = pd.DataFrame(dataset_tags)
    df.to_csv(fname, sep=sep, index=False)
