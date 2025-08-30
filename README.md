!!! Currently depends on the unreleased dev branch of the oem2orm tool WIP !!!!

# oep-upload

Collection of scripts to upload data to OEP

## Use case

### Setup tool & Create tables

- Add your secrets and other settings to the config files for setup
- add your data: Use Frictionless datapackage structure with oemetadata descriptions. Make sure your metadata is valid or at least make sure each table was inferred from the data itself using omi infer method. (INFO: A mapping from frictionless types to database types is applied)
- provide data using CSV or Excel (including a Transformation step when using .xlsx) using the wide format with 1 parameter per cell and one datatype per column (columns might be empty which is okay). Additionally the OEP restrictions are applied which require table names to be shorter then 55 characters and column names < 65 and also all names only use numeric and use "_" for word separation.
- Only the "id" column can be the PrimaryKey for a table, if it does not exists its added automatically. Keep in mind that this overrides your intended definitions.

### Upload data

- Once everything data related from create table step is completed meaning tables exists on the OEP in the "model_draft" section and data is properly formatted as well as file paths are available in the oemetadata you can upload your data automatically. Keep in mind that special cases exists and data types might not be handled. A simple fix requests will in most situation resolve this or other issues quickly.
- Also make sure you properly provided you api credentials in the config files so you have permission to access the OEP-API.

## Usage

You are supposed to pip install (once available) / clone this repo and import its functionality or simply run the main.py file after you completed the setup steps to make all oep-access credentials and the dataset source as well as oemetadata available and readable by the tool.

If you wonder where to put your data you can find a "data" directory where you drop you unzipped folder. In most scenarios there is a directory with the dataset name or acronym. Then below a datapackage.json (which is the oemetadata JSON file) and then a data directory (in case of multiple files) which can include further sub directories to organize the data. In simple cases you might just put the data file next to the json file. Other resources are also allowed but until now they are more or less ignored by the functionality of this tool. To make the upload work it is important to use the wide format of data and comply with the OEP as described in the Setup steps. Here might be questions on how to do that, please just reach out via issue to discuss you case.

Find a file called "config.json" and provide you credentials !!Dont commit!! Production is our public OEP instance which is running at openenergyplatform.org.

Run the main.py file with an installed python environment. I suggest to first install the package manager "uv" on you system and then use it to install this tool. After that you just run the script or import its functionality to create automation pipelines. (Code is documentation for now ... )

## Quick start

You can upload an example datapackage to the OEP if you are already registered there. You can register on openenergyplatform.org and then get you credentials from you profile pages.

Using the example settings in your .env file as well as tool settings you can use the commands in your terminal.

```bash

# i suggest you to use the tool "uv" which you first have to install. It will handle managing python versions and python environments as well as dependencies flawlessly (from my experience everything just installs without issue) You can still use other solutions which can work with the .toml file.

ENV=dev uv run python main.py         # uses local endpoint
ENV=prod uv run python main.py        # uses remote endpoint
```

## Install from source for development or local usage

Install the tool from source:

``` bash

# clone the repo
cd ./ # wherever you want to store the tool

# you need git installed here
git clone https://github.com/jh-RLI/oep-upload.git

# access the cloned directory
cd /oep-upload # make sure you are in the root directory

# install it locally using uv
uv pip install .
```
