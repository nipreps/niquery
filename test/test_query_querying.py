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

import logging
import time
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from niquery.query.querying import (
    edges_to_dataframe,
    fetch_page,
    fetch_pages,
    get_cursors,
    post_with_retry,
    query_dataset_files,
    query_datasets,
    query_snapshot_files,
    query_snapshot_tree,
)
from niquery.utils.attributes import (
    DATASET_DOI,
    DATASETID,
    DIRECTORY,
    FILENAME,
    FULLPATH,
    ID,
    MODALITIES,
    NAME,
    REMOTE,
    SPECIES,
    TAG,
    TASKS,
)


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, raise_http=False):
        self.status_code = status_code
        self._json_data = json_data or {}
        self._raise_http = raise_http

    def raise_for_status(self):
        if self._raise_http:
            http_err = requests.exceptions.HTTPError("HTTP error")
            http_err.response = SimpleNamespace(status_code=self.status_code)
            raise http_err

    def json(self):
        return self._json_data


def test_fetch_page(monkeypatch):
    datasets = {
        "data": {
            "datasets": {
                "edges": [{"node": {"id": "ds000001"}}],
                "pageInfo": {"endCursor": "CUR", "hasNextPage": False},
            }
        }
    }

    def mock_post(url, headers, json):
        return DummyResponse(200, datasets, False)

    monkeypatch.setattr(requests, "post", mock_post)

    out = fetch_page("mygql_url")
    assert "edges" in out and "pageInfo" in out
    assert out["edges"][0]["node"]["id"] == "ds000001"


def test_get_cursors(monkeypatch):
    # Simulate two pages: first says hasNextPage, second does not
    pages = [
        {"edges": [], "pageInfo": {"endCursor": "CUR1", "hasNextPage": True}},
        {"edges": [], "pageInfo": {"endCursor": "CUR2", "hasNextPage": False}},
    ]
    calls = {"i": 0}

    def mock_fetch_page(_, cursor):
        i = calls["i"]
        calls["i"] += 1
        return pages[i]

    monkeypatch.setattr("niquery.query.querying.fetch_page", mock_fetch_page)

    remote = "openneuro"
    cursors = get_cursors(remote)
    assert cursors == [(remote, None), (remote, "CUR1")]


def test_fetch_pages(monkeypatch):
    # Create 3 cursors and return one edge per cursor
    remote = "openneuro"
    cursors = [(remote, None), (remote, "A"), (remote, "B")]

    def mock_fetch_page(gql_url, cursor):
        return {"edges": [{"node": {"id": f"id-{cursor or 'root'}"}}], "pageInfo": {}}

    monkeypatch.setattr("niquery.query.querying.fetch_page", mock_fetch_page)
    out = fetch_pages(cursors, max_workers=2)
    ids = sorted(edge["node"]["id"] for edge in out)
    assert ids == ["id-A", "id-B", "id-root"]


def test_edges_to_dataframe():
    # Include an item None to exercise the guard
    edges = [
        None,
        {
            "node": {
                ID: "ds2",
                NAME: "Dataset 2",
                "metadata": {SPECIES: ""},
                "latestSnapshot": {
                    TAG: "1.0.0",
                    "description": {DATASET_DOI: "10.1/abc"},
                    "summary": {MODALITIES: ["MRI"], TASKS: []},
                },
            }
        },
        {
            "node": {
                ID: "ds1",
                # missing NAME -> NA
                "metadata": {SPECIES: "Homo sapiens"},
                "latestSnapshot": {
                    TAG: "",
                    "description": {},
                    # missing summary -> NA
                },
            }
        },
    ]
    df = edges_to_dataframe(edges)
    # Sorted by id -> ds1 then ds2
    assert list(df["id"]) == ["ds1", "ds2"]
    # Empty string becomes "NA"
    row1 = df[df["id"] == "ds1"].iloc[0]
    assert row1["name"] == "NA"
    assert row1["tag"] == "NA"
    assert row1["datasetdoi"] == "NA"
    assert row1["modalities"] == "NA"
    assert row1["tasks"] == "NA"

    row2 = df[df["id"] == "ds2"].iloc[0]
    assert row2["name"] == "Dataset 2"
    assert row2["species"] == "NA"  # empty -> NA
    assert row2["tag"] == "1.0.0"
    assert row2["datasetdoi"] == "10.1/abc"
    assert row2["modalities"] == ["MRI"]
    assert row2["tasks"] == []


def test_post_with_retry_success(monkeypatch):
    payload = {"query": "x"}

    def mock_post(url, headers, json, timeout):
        return DummyResponse(200, {"ok": True}, False)

    monkeypatch.setattr(requests, "post", mock_post)
    resp = post_with_retry("http://example", {}, payload, retries=3, backoff=1.1)
    assert isinstance(resp, DummyResponse)
    assert resp.json()["ok"] is True


def test_post_with_retry_502_then_success(monkeypatch, caplog):
    sequence = [
        DummyResponse(502, {}, True),  # first attempt -> HTTPError 502
        DummyResponse(200, {"ok": True}, False),  # second attempt -> ok
    ]
    calls = {"i": 0}

    def mock_post(url, headers, json, timeout):
        i = calls["i"]
        calls["i"] += 1
        return sequence[i]

    sleep_calls = []

    def fake_sleep(s):
        sleep_calls.append(s)

    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(time, "sleep", fake_sleep)
    with caplog.at_level(logging.WARNING):
        resp = post_with_retry("http://example", {}, {"q": 1}, retries=3, backoff=2.0)

    assert isinstance(resp, DummyResponse)
    # Ensure backoff was applied for the first retry (2.0**0 == 1.0)
    assert sleep_calls == [1.0]
    assert any("502 Bad Gateway" in rec.message for rec in caplog.records)


def test_post_with_retry_http_error_non_502(monkeypatch, caplog):
    def mock_post(url, headers, json, timeout):
        return DummyResponse(400, {}, True)

    monkeypatch.setattr(requests, "post", mock_post)
    with caplog.at_level(logging.WARNING):
        resp = post_with_retry("http://example", {}, {"q": 1}, retries=2, backoff=1.5)

    assert resp is None
    assert any("HTTPError" in rec.message for rec in caplog.records)


@pytest.mark.parametrize(
    "exc_factory",
    [
        lambda: requests.exceptions.SSLError("ssl"),
        lambda: requests.exceptions.RequestException("req"),
        lambda: Exception("generic"),
    ],
)
def test_post_with_retry_other_exceptions(monkeypatch, exc_factory, caplog):
    def mock_post(url, headers, json, timeout):
        raise exc_factory()

    monkeypatch.setattr(requests, "post", mock_post)
    with caplog.at_level(logging.WARNING):
        resp = post_with_retry("http://example", {}, {"q": 1})

    assert resp is None
    # A warning was logged
    assert any("for http://example" in rec.message for rec in caplog.records)


def test_query_snapshot_files_response_none(monkeypatch, caplog):
    def mock_post_with_retry(gql_url, headers, payload):
        return None

    monkeypatch.setattr("niquery.query.querying.post_with_retry", mock_post_with_retry)
    with caplog.at_level(logging.WARNING):
        files = query_snapshot_files("mygql_url", "ds000001", "1.0.0")
    assert files == []
    assert any("Empty response" in rec.message for rec in caplog.records)


def test_query_snapshot_files_snapshot_none(monkeypatch, caplog):
    def mock_post_with_retry(gql_url, headers, payload):
        return DummyResponse(200, {"data": {"snapshot": None}})

    monkeypatch.setattr("niquery.query.querying.post_with_retry", mock_post_with_retry)
    with caplog.at_level(logging.WARNING):
        files = query_snapshot_files("mygql_url", "ds000001", "1.0.0")
    assert files == []
    assert any("No snapshot" in rec.message for rec in caplog.records)


def test_query_snapshot_files_ok(monkeypatch):
    files_payload = {
        "data": {"snapshot": {"files": [{"id": "x", "filename": "a", "directory": False}]}}
    }

    def mock_post_with_retry(url, headers, payload):
        # Ensure variables passed are preserved
        assert payload["variables"]["datasetId"] == "ds000001"
        assert payload["variables"]["tag"] == "1.0.0"
        assert payload["variables"]["tree"] is None
        return DummyResponse(200, files_payload)

    monkeypatch.setattr("niquery.query.querying.post_with_retry", mock_post_with_retry)
    files = query_snapshot_files("mygq_url", "ds000001", "1.0.0")
    assert files == [{"id": "x", "filename": "a", "directory": False}]


def test_query_snapshot_tree_recurses(monkeypatch):
    # First call returns a directory and a file
    root_files = [
        {ID: "dir1", FILENAME: "sub", DIRECTORY: True},
        {ID: "f1", FILENAME: "rootfile.txt", DIRECTORY: False},
    ]
    # Second call returns a file within the directory
    sub_files = [
        {ID: "f2", FILENAME: "subfile.txt", DIRECTORY: False},
    ]
    calls = []

    def mock_query_snapshot_files(gql_url, ds, tag, tree):
        calls.append(tree or "root")
        if tree is None:
            return root_files
        elif tree == "dir1":
            return sub_files
        return []

    monkeypatch.setattr("niquery.query.querying.query_snapshot_files", mock_query_snapshot_files)
    out = query_snapshot_tree("mygq_url", "ds000001", "1.0.0")
    # Should include only file entries with fullpath
    fullpaths = sorted(f[FULLPATH] for f in out)
    assert fullpaths == ["rootfile.txt", "sub/subfile.txt"]
    assert calls == ["root", "dir1"]


def test_query_snapshot_tree_handles_exception(monkeypatch, caplog):
    def bad_query(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr("niquery.query.querying.query_snapshot_files", bad_query)
    with caplog.at_level(logging.WARNING):
        out = query_snapshot_tree("mygq_url", "ds000001", "1.0.0")
    assert out == []
    assert any("Failed to query" in rec.message for rec in caplog.records)


def test_query_dataset_files_empty_tag_logs(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING):
        out = query_dataset_files("mygq_url", "ds000001", "NA")
    assert out == []
    assert any("Snapshot empty" in rec.message for rec in caplog.records)


def test_query_dataset_files_success_and_exception(monkeypatch, caplog):
    def ok_tree(gql_url, ds, tag):
        return [
            {ID: "f1", FILENAME: "a.nii.gz", DIRECTORY: False, FULLPATH: "a.nii.gz"},
            {ID: "f2", FILENAME: "b.nii.gz", DIRECTORY: False, FULLPATH: "b.nii.gz"},
        ]

    def bad_tree(gql_url, ds, tag):
        raise RuntimeError("err")

    # Success case
    monkeypatch.setattr("niquery.query.querying.query_snapshot_tree", ok_tree)
    files = query_dataset_files("mygq_url", "ds000001", "1.0.0")
    assert len(files) == 2

    # Exception case
    monkeypatch.setattr("niquery.query.querying.query_snapshot_tree", bad_tree)
    with caplog.at_level(logging.WARNING):
        files = query_dataset_files("mygq_url", "ds000002", "2.0.0")
    assert files == []
    assert any("Post request error" in rec.message for rec in caplog.records)


def test_query_datasets_success_empty_and_exception(monkeypatch):
    # DataFrame with three rows
    remote = "openneuro"
    df = pd.DataFrame(
        [
            {REMOTE: remote, ID: "ds1", TAG: "1.0.0"},
            {REMOTE: remote, ID: "ds2", TAG: "2.0.0"},
            {REMOTE: remote, ID: "ds3", TAG: "3.0.0"},
        ]
    )

    def mock_query_dataset_files(gql_url, dataset_id, snapshot_tag):
        if dataset_id == "ds1":
            return [
                {ID: "f1", FILENAME: "a.nii.gz", DIRECTORY: False, FULLPATH: "a.nii.gz"},
                {ID: "f2", FILENAME: "b.nii.gz", DIRECTORY: False, FULLPATH: "b.nii.gz"},
            ]
        if dataset_id == "ds2":
            return []  # empty -> failure_results
        raise RuntimeError("oops")  # -> failure_results

    monkeypatch.setattr("niquery.query.querying.query_dataset_files", mock_query_dataset_files)

    success, failures = query_datasets(df, max_workers=2)

    # Success contains ds1 only, with dataset/tag info merged
    assert list(success.keys()) == ["ds1"]
    ds1_files = success["ds1"]
    # Ensure FULLPATH sorting is applied
    assert [x[FULLPATH] for x in ds1_files] == ["a.nii.gz", "b.nii.gz"]
    # Ensure dataset context merged
    assert all(x[DATASETID] == "ds1" and x[TAG] == "1.0.0" for x in ds1_files)

    # Failures for ds2 (empty) and ds3 (exception)
    assert failures == [
        {REMOTE: remote, DATASETID: "ds2", TAG: "2.0.0"},
        {REMOTE: remote, DATASETID: "ds3", TAG: "3.0.0"},
    ]
