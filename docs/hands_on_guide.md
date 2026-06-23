# OEP-Upload tool hands on Guide

This guide is a minimal working example on how to use the oep-upload tool to work with you local CSV data, create a datapackage.json or use your existing one and upload your data and metadata to the openenergyplatform.org.

## Step by step

1 Prepare your data
2 Set the oep-upload configuration settings so it knows where to look for your data and has permissions to upload data to the OEP
3 Run the tool, monitor the logs and fix errors in the data: Mostly bad definition of types or mixed types in columns
4 Repeat until upload was successful

## 1 Prepare your data

To make things consistent we enforce a few constrains on how to organize the data. On the baseline we want to be in line with the frictionless data package v2 specification. As we developed our domain specific metadata called oemetadata we require you to use it as the datapackage.json which is specified in frictionless. The oemetadata is already in line with the frictionless spec but provides a comprehensive set of metadata properties we see as most relevant for you to provide. The oemetadata specification also explains what properties should be available and explains their purpose as well as example values and more.

Practically we want you to use a datapackage directory structure which is setup like this:

- A "parent" directory which is named after the dataset.
- It contains a "datapackage.json" and a "data/" subdirectory.
- Weather you have a single or multiple data files you place everything in the data directory. IF you want you can use further subdirectories. The tool will only handle tabular data in the form of "CSV"

## 2 Configure the oep-upload tool

The upload of data to the OEP using the API requires some settings, most of them are handled by the tool already and must only be changed if you are a developer who wants to use a test OEP setup. There are some settings which are relevant for you as a user to adapt to you specific use:

- The easiest way is to scaffold the two config files in your project folder:

  ```bash
  oep-upload init      # writes settings.local.yaml + .env into the current folder
  ```

- Add your OEP-API token in the created `.env` under `OEP_API_TOKEN`.
- Tell the tool where your data lives by editing `settings.local.yaml`. Settings
  are layered (lowest to highest): `settings.base.yaml` < `settings.<env>.yaml` <
  packaged `settings.local.yaml` < **project `settings.local.yaml` (your working
  directory)** < environment variables / `.env`.

  ```yaml
  # ./settings.local.yaml  (in the folder you run oep-upload from)
  api:
    target: remote   # or "local" if you run a local OEP

  paths:
    # Folder that holds your datapackage. Relative paths are resolved from where
    # you run the tool. `~` and $ENV_VARS are expanded.
    root: data/my_dataset
    # Optional: where the data files are, relative to root (defaults to root).
    # data_dir: .
    # Optional: the datapackage.json, relative to data_dir (or absolute).
    datapackage_file: datapackage.json
  ```

  You only need `root` in the simple case. `data_dir` and `datapackage_file` are
  resolved relative to `root`; an absolute value simply overrides the parent.

- Verify the tool picked everything up:

  ```bash
  oep-upload config    # shows the resolved settings and which files were loaded
  ```

## 3 Run the tool

Currently you have to install the oep-upload tool, please use the python package manager tooling `uv` for that. We currently use `Python v3.13` which can also be installed using `uv` if you don´t have it available already.

- Close the oep-upload github repository and navigate into the cloned directory

  - git clone <https://github.com/jh-RLI/oep-upload>
  - cd oep-upload

- Install `uv`, create a new python environment with python 3.13 and install the tool dependencies:

  - uv venv --python 3.13
  - source venv/bin/activate
  - uv pip install .

- Run the tool (any of these are equivalent):

  - oep-upload
  - python -m oep_upload
  - python main.py

## 4 Fix errors in the config, data and metadata & Repeat running the tool

As the tool is still experimental don´t expect a full fledged perfect user experience. It helps with a lot of basic steps and also introduces many automatically applied helpers so you can get up to speed with the OEP faster. It cannot fix the fundamental issues in your data structure / contents or metadata. The process of preparing your data to be able to upload it can be quite extensive and often requires some experience. Please reach out to us in the GitHub issues if you cant figure out how to fix your data flaws or where to start.

- Check if you use the correct CSV delimiter. E. g. if you use multiple CSV files make sure in the metadata (datapackage.json) you provide the correct delimiter for each resource.
- Check weather you table and column names are in line with postgresql and OEP restrictions:

  - Only use alphanumerical characters
  - Only use "_" to separate word
  - Keep them below 50 characters
  - Specify the ID column in the metadata always to a column "id" as the OEP will add it if you don´t have it yet available in your data. Only "id" columns can be the unique identifier other column names are not allowed.
  - Foreign keys must reference a column from a table that exists in your datapackage
  - Make sure to specify the correct data type, never use "any". The tool will auto infer metadata like the data type if not available already in the datapackage.json. If columns have a "any" type this means the column holds multiple data types. In this case a upload will not work because the database system restricts this. Only one data type per column is allowed

What changes will be auto. applied to the data:

- Table names longer than 50 characters will be truncated
- Column names longer than 50 characters will be truncated
