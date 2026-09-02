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

import gzip
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3  # type: ignore
import nibabel as nb
import pandas as pd
import requests
from botocore import UNSIGNED  # type: ignore
from botocore.config import Config  # type: ignore
from tqdm import tqdm

from niquery.data.remotes import BUCKET, REMOTES
from niquery.utils.attributes import (
    DATASETID,
    FULLPATH,
    REMOTE,
    VOLS,
    VOXEL_ANISOTROPY,
    VOXEL_SZ_X,
    VOXEL_SZ_Y,
    VOXEL_SZ_Z,
)

NBYTES = 512
BYTE_RANGE = f"bytes=0-{NBYTES}"

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))


def _get_nii_header_bytes(data: bytes) -> nb.nifti1.Nifti1Header:
    """Get the NIfTI header from the provided byte data.

    Parameters
    ----------
    data : :obj:`bytes`
        Bytes containing the NIfTI file header.

    Returns
    -------
    :obj:`~nibabel.nifti1.Nifti1Header`
        NIfTI file header.
    """

    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as img:
        return nb.Nifti1Image.from_stream(img).header


def get_nii_header_s3(bucket: str, filename: str) -> nb.nifti1.Nifti1Header:
    """Get the NIfTI header of the given file from the s3 bucket.

    Parameters
    ----------
    bucket : :obj:`str`
        S3 bucket.
    filename : :obj:`str`
        NIfTI filename (e.g.
        'ds000149/sub-01/func/sub-01_task-picturemanualresponse_run-01_bold.nii.gz')

    Returns
    -------
    :obj:`~nibabel.nifti1.Nifti1Header`
        NIfTI file header.
    """

    response = s3.get_object(Bucket=bucket, Key=filename, Range=BYTE_RANGE)
    data = response["Body"].read()

    return _get_nii_header_bytes(data)


def get_nii_voxelsize_s3(bucket: str, filename: str):
    """Return the voxel size (pixdim[1:4]) from the NIfTI header on S3."""
    header = get_nii_header_s3(bucket, filename)
    return header["pixdim"][1:4]


def get_nii_anisotropy_s3(bucket: str, filename: str):
    """Return the anisotropy ratio (max/min of voxel sizes) from NIfTI header."""
    voxsz = get_nii_voxelsize_s3(bucket, filename)
    return float(np.max(voxsz) / np.min(voxsz)) if np.min(voxsz) > 0 else np.nan


def get_nii_timepoints_s3(bucket: str, filename: str) -> int:
    """Compute the number of timepoints of the provided NIfTI file.

    Computes the number of timepoints as the size along the last dimension from
    the header of the response bitstream without actually downloading the entire
    contents if the server supports Range requests.

    Parameters
    ----------
    bucket : :obj:`str`
        S3 bucket.
    filename : :obj:`str`
        NIfTI filename (e.g.
        'ds000149/sub-01/func/sub-01_task-picturemanualresponse_run-01_bold.nii.gz')

    Returns
    -------
    :obj:`int`
        Number of timepoints.
    """

    header = get_nii_header_s3(bucket, filename)
    return header["dim"][4]


def get_nii_header_url(url: str) -> nb.nifti1.Nifti1Header:
    """Get the NIfTI header of the file pointed by the given URL.

    Parameters
    ----------
    url : :obj:`str`
        URL where the file of interest is located.

    Returns
    -------
    :obj:`~nibabel.nifti1.Nifti1Header`
        NIfTI file header.
    """

    response = requests.get(url, headers={"Range": BYTE_RANGE})
    if response.status_code not in (200, 206):
        raise RuntimeError(f"Failed to fetch byte range from URL: {response.status_code}")

    data = response.content

    return _get_nii_header_bytes(data)


def get_nii_timepoints_url(url: str) -> int:
    """Compute the number of timepoints of the file pointed by the given URL.

    Computes the number of timepoints as the size along the last dimension from
    the header of the response bitstream without actually downloading the entire
    contents if the server supports Range requests.

    Parameters
    ----------
    url : :obj:`str`
        URL where the file of interest is located.

    Returns
    -------
    :obj:`int`
        Number of timepoints.
    """

    header = get_nii_header_url(url)
    return header["dim"][4]


def extract_volume_features(files: dict, max_workers: int = 8) -> tuple:
    """Extract the number of volumes.

    Extracts the number of volumes for all files runs in each dataset.

    Parameters
    ----------
    files : :obj:`dict`
        Dataset records.
    max_workers : :obj:`int`, optional
        Maximum number of parallel threads to use.

    Returns
    -------
    :obj:`tuple`
        A dictionary of dataset records with the extracted BOLD features, and a
        list of failed dataset ID and file paths.
    """

    success_results: dict[str, list[pd.Series]] = {dataset_id: [] for dataset_id in files}
    failure_results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for dataset_id, df in files.items():
            for _, rec in df.iterrows():
                futures[
                    executor.submit(
                        get_nii_timepoints_s3,
                        REMOTES[rec[REMOTE]][BUCKET],
                        str(Path(dataset_id) / Path(rec[FULLPATH])),
                    )
                ] = (dataset_id, rec)
                futures[
                    executor.submit(
                        get_nii_voxelsize_s3,
                        REMOTES[rec[REMOTE]][BUCKET],
                        str(Path(dataset_id) / Path(rec[FULLPATH])),
                    )
                ] = (dataset_id, rec, "voxsz")
                futures[
                    executor.submit(
                        get_nii_anisotropy_s3,
                        REMOTES[rec[REMOTE]][BUCKET],
                        str(Path(dataset_id) / Path(rec[FULLPATH])),
                    )
                ] = (dataset_id, rec, "anisotropy")

        # Collect results, assuming all three futures per file complete
        temp_results = {}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting features"):
            dataset_id, rec = futures[future]
            dataset_id, rec, key = futures[future]
            try:
                n_vols = future.result()
                rec_vols = rec.copy()
                rec_vols[VOLS] = n_vols
                success_results[dataset_id].append(rec_vols)

                value = future.result()
                rec_id = (dataset_id, rec[FULLPATH])
                if rec_id not in temp_results:
                    temp_results[rec_id] = rec.copy()
                if key == "vols":
                    temp_results[rec_id][VOLS] = value
                elif key == "voxsz":
                    temp_results[rec_id][VOXEL_SZ_X] = value[0]
                    temp_results[rec_id][VOXEL_SZ_Y] = value[1]
                    temp_results[rec_id][VOXEL_SZ_Z] = value[2]
                elif key == "anisotropy":
                    temp_results[rec_id][VOXEL_ANISOTROPY] = value

            except Exception as e:
                logging.warning(f"Failed to process {dataset_id}:{rec[FULLPATH]}: {e}")
                failure_results.append(
                    {REMOTE: rec[REMOTE], DATASETID: dataset_id, FULLPATH: rec[FULLPATH]}
                )

        # Push to final results by dataset
        for (dataset_id, _), rec_full in temp_results.items():
            success_results[dataset_id].append(rec_full)

    # Sort results before returning
    return {
        k: sorted(v, key=lambda s: s[FULLPATH]) for k, v in sorted(success_results.items())
    }, sorted(failure_results, key=lambda x: (x[DATASETID], x[FULLPATH]))
