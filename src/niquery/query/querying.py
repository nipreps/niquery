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
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from tqdm import tqdm

from niquery.data.remotes import OPENNEURO_GRAPHQL_URL
from niquery.utils.attributes import (
    DATASET_DOI,
    DATASETID,
    DIRECTORY,
    FILENAME,
    FULLPATH,
    ID,
    MODALITIES,
    NAME,
    SPECIES,
    TAG,
    TASKS,
)

HEADERS = {"Content-Type": "application/json"}

MAX_QUERY_SIZE = 100
"""Maximum page size."""


def fetch_page(after_cursor: str | None = None) -> dict:
    """Fetch a single page of OpenNeuro datasets using GraphQL.

    Parameters
    ----------
    after_cursor : :obj:`str`, optional
        The pagination cursor indicating where to start. If ``None``, fetches
        the first page.

    Returns
    -------
    :obj:`dict`
         Dictionary with keys 'edges' (list of datasets) and 'pageInfo'
         (pagination metadata).
    """

    query = """
    query DatasetsWithLatestSnapshots($after: String, $first: Int!) {
      datasets(first: $first, after: $after, orderBy: { created: ascending }) {
        edges {
          node {
            id
            name
            metadata {
              species
            }
            latestSnapshot {
              tag
              description {
                DatasetDOI
              }
              summary {
                modalities
                tasks
              }
            }
          }
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }
    """

    variables = {"after": after_cursor, "first": MAX_QUERY_SIZE}
    response = requests.post(
        OPENNEURO_GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": variables}
    )
    response.raise_for_status()
    return response.json()["data"]["datasets"]


def get_cursors() -> list:
    """Serially walk through the entire OpenNeuro dataset list to collect all pagination cursors.

    This function starts from the beginning and keeps fetching pages until the
    last one, recording the 'endCursor' of each page to enable parallel fetching
    later.

    Returns
    -------
    cursors : :obj:`list`
        List of cursors, where the first cursor is ``None`` (start of list), and
        the rest are page markers returned by GraphQL.
    """

    cursors = [None]
    current_cursor = None
    with tqdm(desc="Discovering cursors", unit="page") as pbar:
        while True:
            data = fetch_page(current_cursor)
            page_info = data["pageInfo"]
            if page_info["hasNextPage"]:
                current_cursor = page_info["endCursor"]
                cursors.append(current_cursor)
                pbar.update(1)
            else:
                break
    return cursors


def fetch_pages(cursors: list, max_workers: int = 8) -> list:
    """Fetch all OpenNeuro dataset pages in parallel using a precomputed list of cursors.

    Parameters
    ----------
    cursors : :obj:`list`
        List of cursors.
    max_workers : :obj:`int`, optional
        Maximum number of parallel threads to use.

    Returns
    -------
    results : :obj:`list`
        List of datasets.
    """

    logging.info(f"Querying {OPENNEURO_GRAPHQL_URL}...")

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_page, cursor): cursor for cursor in cursors}
        with tqdm(total=len(futures), desc="Fetching pages", unit="page") as pbar:
            for future in as_completed(futures):
                data = future.result()
                results.extend(edge for edge in data["edges"] if edge is not None)
                pbar.update(1)
    return results


def edges_to_dataframe(edges: list) -> pd.DataFrame:
    """Convert a list of dataset edges (GraphQL response) into a pandas DataFrame.

    Returned values are sorted by the dataset 'id'.

    Parameters
    ----------
    edges : :obj:`list`
        GraphQL edges. Each edge contains a 'node' with dataset metadata.

    Returns
    -------
    :obj:`~pd.DataFrame`
        A DataFrame with the relevant dataset information, namely 'id', 'name',
        'species', 'tag', 'dataset_doi', 'modalities', and 'tasks'.
    """

    rows = []
    for item in edges:
        if item is None:
            continue
        node = item["node"]
        snapshot = node.get("latestSnapshot", {})
        row = {
            ID.lower(): node.get(ID),
            NAME.lower(): node.get(NAME, None),
            SPECIES.lower(): node.get("metadata", None).get(SPECIES),
            TAG.lower(): snapshot.get(TAG),
            DATASET_DOI.lower(): snapshot.get("description", {}).get(DATASET_DOI),
            MODALITIES.lower(): snapshot.get("summary", {}).get(MODALITIES)
            if snapshot.get("summary")
            else None,
            TASKS.lower(): snapshot.get("summary", {}).get(TASKS)
            if snapshot.get("summary")
            else None,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    # Replace all empty strings by "NA"
    df.replace("", "NA", inplace=True)
    return df.fillna("NA").sort_values("id")


def post_with_retry(
    url: str, headers: dict, payload: dict, retries: int = 5, backoff: float = 1.5
) -> requests.Response | None:
    """Post an HTTP request with retrying.

    If the request is unsuccessful, retry ``retries`` times after an exponential
    wait time computed as :math:`backoff^attempt`.

    Parameters
    ----------
    url : :obj:`str`
        URL to post to.
    headers : :obj:`dict`
        HTTP headers.
    payload : :obj:`dict`
        HTTP payload.
    retries : :obj:`int`, optional
        Number of retry attempts.
    backoff : :obj:`float`, optional
        Retry delay.

    Returns
    -------
    :obj:`requests.Response` or None
        Request response. ``None`` if attempts failed.
    """

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status == 502 and attempt < retries - 1:
                wait = backoff**attempt
                logging.warning(f"502 Bad Gateway, retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                logging.warning(f"HTTPError for {url}: {e}")
                return None
        except requests.exceptions.SSLError as e:
            logging.warning(f"SSLError for {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logging.warning(f"RequestException for {url}: {e}")
            return None
        except Exception as e:
            logging.warning(f"Request failed for {url}: {e}")
            return None

    return None


def query_snapshot_files(dataset_id: str, snapshot_tag: str, tree: str | None = None) -> list:
    """Query the list of files at a specific level of a dataset snapshot.

    Parameters
    ----------
    dataset_id : :obj:`str`
        The OpenNeuro dataset ID (e.g., 'ds000001').
    snapshot_tag : :obj:`str`
        The tag of the snapshot to query (e.g., '1.0.0').
    tree : :obj:`str`, optional
        ID of a directory within the snapshot tree to query; use ``None`` to
        start at the root.

    Returns
    -------
    :obj:`list`
        Each dict represents a file or directory with fields 'id', 'filename',
        'size', 'directory', 'annexed', 'key', and 'urls'.
    """

    query = """
    query getSnapshotFiles($datasetId: ID!, $tag: String!, $tree: String) {
      snapshot(datasetId: $datasetId, tag: $tag) {
        files(tree: $tree) {
          id
          filename
          size
          directory
          annexed
          key
          urls
        }
      }
    }
    """

    variables = {"datasetId": dataset_id, "tag": snapshot_tag, "tree": tree}
    response = post_with_retry(
        OPENNEURO_GRAPHQL_URL, HEADERS, {"query": query, "variables": variables}
    )

    # Ensure that the JSON response object contains all required keys
    if response is None:
        logging.warning(f"Empty response for {dataset_id}:{snapshot_tag}")
        return []

    json_response = response.json()
    snapshot = json_response.get("data", {}).get("snapshot")

    if snapshot is None:
        logging.warning(f"No snapshot returned for {dataset_id}:{snapshot_tag}")
        return []

    return snapshot.get("files", []) or []


def query_snapshot_tree(
    dataset_id: str, snapshot_tag: str, tree: str | None = None, parent_path=""
) -> list:
    """Recursively query all files in an OpenNeuro dataset snapshot.

    Parameters
    ----------
    dataset_id : :obj:`str`
        The OpenNeuro dataset ID (e.g., 'ds000001').
    snapshot_tag : :obj:`str`
        The tag of the snapshot to query (e.g., '1.0.0').
    tree : :obj:`str`, optional
        ID of a directory within the snapshot tree to query; use ``None`` to
        start at the root.
    parent_path : :obj:`str`, optional
        Relative path used to construct full file paths (used during recursion).

    Returns
    -------
    all_files : :obj:`list`
        List of all file entries (not directories), each including a 'fullpath'
        key that shows the complete path from the root.
    """

    all_files = []

    try:
        files = query_snapshot_files(dataset_id, snapshot_tag, tree)
    except Exception as e:
        logging.warning(f"Failed to query {dataset_id}:{snapshot_tag} at tree {tree}: {e}")
        return []

    for f in files:
        current_path = f"{parent_path}/{f[FILENAME]}".lstrip("/")
        if f[DIRECTORY]:
            sub_files = query_snapshot_tree(
                dataset_id, snapshot_tag, f[ID], parent_path=current_path
            )
            all_files.extend(sub_files)
        else:
            f[FULLPATH] = current_path
            all_files.append(f)

    return all_files


def query_dataset_files(dataset_id: str, snapshot_tag: str) -> list:
    """Retrieve all files for a given OpenNeuro dataset snapshot.

    This function takes a dataset metadata dictionary (typically a row from a
    :obj:`~pd.DataFrame`), extracts the dataset ID and snapshot tag, and
    recursively queries all files in the snapshot. If the snapshot tag is
    missing or the request fails, an empty list is returned.

    Parameters
    ----------
    dataset_id : :obj:`str`
        Dataset ID (e.g., 'ds000001').
    snapshot_tag : :obj:`str`
        Snapshot tag (e.g., '1.0.0').

    Returns
    -------
    :obj:`list`
        List of files containing their metadata dictionaries, each including the
        fields 'id', 'filename', 'size', 'directory', 'annexed', 'key', 'urls',
        and 'fullpath'.

    Notes
    -----
    - If 'tag' is missing or marked as ``NA``, no files are returned.
    - Errors during querying are caught and logged, returning an empty list.
    """

    if not snapshot_tag or snapshot_tag == "NA":
        logging.warning(f"Snapshot empty for {dataset_id}")
        return []

    try:
        files = query_snapshot_tree(dataset_id, snapshot_tag)
    except Exception as e:
        logging.warning(f"Post request error for {dataset_id}:{snapshot_tag}: {e}")
        return []

    return files


def query_datasets(df: pd.DataFrame, max_workers: int = 8) -> tuple:
    """Perform file queries over a DataFrame of datasets.

    Parameters
    ----------
    df : :obj:`~pd.DataFrame`
        Dataset records.
    max_workers : :obj:`int`, optional
        Maximum number of parallel threads to use.

    Returns
    -------
    :obj:`tuple`
        A mapping from dataset ID to list of file metadata dictionaries, and a
        list of failed dataset ID and snapshot tags.
    """

    logging.info(f"Querying {OPENNEURO_GRAPHQL_URL}...")

    success_results = {}
    failure_results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(query_dataset_files, row[ID], row[TAG]): (row[ID], row[TAG])
            for _, row in df.iterrows()
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing datasets"):
            dataset_id, snapshot_tag = futures[future]
            try:
                result = future.result(timeout=20)
                if result:
                    success_results[dataset_id] = [
                        {DATASETID: dataset_id, TAG: snapshot_tag} | file for file in result
                    ]
                else:
                    logging.warning(f"Empty result for {dataset_id}:{snapshot_tag}")
                    failure_results.append({DATASETID: dataset_id, TAG: snapshot_tag})
            except TimeoutError:
                logging.info(f"Timeout for {dataset_id}:{snapshot_tag}")
                failure_results.append({DATASETID: dataset_id, TAG: snapshot_tag})
            except Exception as e:
                logging.info(f"Failed to process {dataset_id}:{snapshot_tag}: {e}")
                failure_results.append({DATASETID: dataset_id, TAG: snapshot_tag})

    # Sort results before returning
    return {
        k: sorted(v, key=lambda s: s[FULLPATH]) for k, v in sorted(success_results.items())
    }, sorted(failure_results, key=lambda x: (x[DATASETID], x[TAG]))
