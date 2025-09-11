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

import importlib.metadata
import logging
import os
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from click.testing import CliRunner

from niquery.__main__ import cli
from niquery.data.utils import bids_dataset_name_pattern
from niquery.io.utils import append_label_to_filename
from niquery.utils.attributes import DATASETID
from niquery.utils.optpckg import have_datalad

entry_points = importlib.metadata.entry_points(group="console_scripts")
cli_name = [ep.name for ep in entry_points if ep.value.startswith("niquery.cli.run:cli")][0]


def test_main_help():
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"], prog_name=cli_name)
    assert result.exit_code == 0
    assert result.output.startswith(f"Usage: {cli_name} [OPTIONS] COMMAND [ARGS]")


def test_index_help():
    cmd_str = "index"
    runner = CliRunner()
    result = runner.invoke(cli, [cmd_str, "--help"], prog_name=cli_name)
    assert result.exit_code == 0
    assert result.output.startswith(f"Usage: {cli_name} {cmd_str} [OPTIONS] REMOTE OUT_FILENAME")


def test_index_run(tmp_path):
    cmd_str = "index"
    runner = CliRunner()
    remote = "openneuro"
    out_fname = tmp_path / "openneuro_datasets.tsv"
    result = runner.invoke(cli, [cmd_str, remote, str(out_fname)], prog_name=cli_name)
    assert result.exit_code == 0
    assert out_fname.is_file()


def test_collect_help():
    cmd_str = "collect"
    runner = CliRunner()
    result = runner.invoke(cli, [cmd_str, "--help"], prog_name=cli_name)
    assert result.exit_code == 0
    assert result.output.startswith(
        f"Usage: {cli_name} {cmd_str} [OPTIONS] IN_FILENAME OUT_DIRNAME"
    )


def test_collect_run(tmp_path):
    cmd_str = "collect"
    runner = CliRunner()
    in_fname = Path(os.getenv("TEST_DATA_HOME")) / "openneuro_datasets_sample.tsv"
    out_dirname = tmp_path / "dataset_files"
    os.makedirs(out_dirname, exist_ok=False)
    result = runner.invoke(
        cli,
        [
            cmd_str,
            str(in_fname),
            str(out_dirname),
            "--species",
            "human",
            "--modality",
            "bold",
            "--modality",
            "fmri",
            "--modality",
            "mri",
        ],
        prog_name=cli_name,
    )
    assert result.exit_code == 0
    fname = append_label_to_filename(in_fname, "relevant")
    assert fname.is_file()
    expected_ds_count = 11
    pattern = bids_dataset_name_pattern()
    assert (
        len(
            [
                entry
                for entry in out_dirname.iterdir()
                if entry.is_file() and pattern.search(entry.name)
            ]
        )
        == expected_ds_count
    )


def test_analyze_help():
    cmd_str = "analyze"
    runner = CliRunner()
    result = runner.invoke(cli, [cmd_str, "--help"], prog_name=cli_name)
    assert result.exit_code == 0
    assert result.output.startswith(
        f"Usage: {cli_name} {cmd_str} [OPTIONS] IN_DIRNAME OUT_DIRNAME"
    )


def test_analyze_run(tmp_path):
    cmd_str = "analyze"
    runner = CliRunner()
    in_dirname = Path(os.getenv("TEST_DATA_HOME")) / "dataset_files"
    out_dirname = tmp_path / "dataset_features"
    os.makedirs(out_dirname, exist_ok=False)
    result = runner.invoke(
        cli,
        [cmd_str, str(in_dirname), str(out_dirname), "--suffix", "bold"],
        prog_name=cli_name,
    )
    assert result.exit_code == 0
    expected_ds_count = 10
    pattern = re.compile(r"ds\d{6}\.tsv")
    assert (
        len(
            [
                entry
                for entry in out_dirname.iterdir()
                if entry.is_file() and pattern.search(entry.name)
            ]
        )
        == expected_ds_count
    )


def test_select_help():
    cmd_str = "select"
    runner = CliRunner()
    result = runner.invoke(cli, [cmd_str, "--help"], prog_name=cli_name)
    assert result.exit_code == 0
    assert result.output.startswith(
        f"Usage: {cli_name} {cmd_str} [OPTIONS] IN_DIRNAME OUT_FILENAME SEED"
    )


def test_select_run(tmp_path):
    cmd_str = "select"
    runner = CliRunner()
    in_dirname = Path(os.getenv("TEST_DATA_HOME")) / "dataset_features"
    out_fname = tmp_path / "selected_openneuro_datasets.tsv"
    seed = 1234
    total_runs = 30
    contr_fraction = 0.2
    min_timepoints = 300
    max_timepoints = 1200
    result = runner.invoke(
        cli,
        [
            cmd_str,
            str(in_dirname),
            str(out_fname),
            str(seed),
            "--total-runs",
            str(total_runs),
            "--contr-fraction",
            str(contr_fraction),
            "--min-timepoints",
            str(min_timepoints),
            "--max-timepoints",
            str(max_timepoints),
        ],
        prog_name=cli_name,
    )
    assert result.exit_code == 0
    assert out_fname.is_file()
    expected_ds_count = 5
    assert len(np.unique(pd.read_csv(out_fname, sep="\t")[DATASETID].values)) == expected_ds_count


def test_aggregate_help():
    cmd_str = "aggregate"
    runner = CliRunner()
    result = runner.invoke(cli, [cmd_str, "--help"], prog_name=cli_name)
    assert result.exit_code == 0
    assert result.output.startswith(
        f"Usage: {cli_name} {cmd_str} [OPTIONS] IN_FILENAME OUT_DIRNAME"
    )


@pytest.mark.skipif(not have_datalad(), reason="Missing DataLad or system dependencies")
def test_aggregate_run(tmp_path, caplog):
    cmd_str = "aggregate"
    runner = CliRunner()
    in_filename = Path(os.getenv("TEST_DATA_HOME")) / "selected_openneuro_datasets_sample.tsv"
    out_dirname = tmp_path / "aggregate_dataset"
    os.makedirs(out_dirname, exist_ok=False)
    dataset_name = "ds000001"

    with caplog.at_level(logging.INFO):
        result = runner.invoke(
            cli,
            [
                cmd_str,
                str(in_filename),
                str(out_dirname),
                dataset_name,
            ],
        )
    # The command should finish without error
    assert result.exit_code == 0
    # Output should mention aggregation (success/failure), but exact text may vary with real fetch
    assert any("Aggregated" in rec.message for rec in caplog.records)
