.. include:: links.rst

How to Use
==========

*NiQuery* is a command-line tool for indexing, collecting,analyzing,
and selecting fMRI BOLD datasets from a remote neuroimaging data
server, such as OpenNeuro. The typical workflow consists of four
stages:

  #. **Index**: Retrieve available datasets and snapshot information
     from the remote server.
  #. **Collect**: Gather per-dataset fMRI BOLD file listings.
  #. **Analyze**: Extract features from collected files.
  #. **Select**: Choose datasets that satisfy specific constraints.
  #. **Aggregate**: Aggregate into a DataLad dataset the selected
     datasets.

Below are example calls to perform the *NiQuery* actions:

**Index**::

  niquery index openneuro openneuro_datasets.tsv

**Collect**::

  niquery collect openneuro_datasets.tsv ./dataset_files

``collect`` traverses all trees for all datasets of interest in order
to gather the paths to the ``NIfTI`` files. Note that this process can
take several hours.

**Analyze**::

  niquery analyze ./dataset_files ./dataset_features

**Select**::

  niquery select ./dataset_features selected_openneuro_datasets.tsv 1234 \
    --total-runs 4000 \
    --contr-fraction 0.05 \
    --min-timepoints 300 \
    --max-timepoints 1200

**Aggregate**::

  niquery aggregate selected_openneuro_datasets.tsv ./aggregated_datasets dsresearch
