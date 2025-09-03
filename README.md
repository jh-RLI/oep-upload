!!! Currently depends on the unreleased dev branch of the oem2orm tool WIP !!!!

# oep-upload

Experimental tool to handle the full use case of data uploads to the oeplatform:

- (Note yet implemented) `oep_upload.describe` Automatically infer metadata about columns and data types from you data files (CSV)
- `oep_upload.create` Read resource (table) definitions from oemetadata JSON file, Normalize table and column names to meet OEP constrains, construct a SQLAlchemy ORM of the table using `oem2orm` including constrains like PK, FK and Unique for fields which are not PK but referenced as FK in other resources and of course create the table on the oeplatform.
- `oep_upload.upload` Read resource (table) data paths from oemetadata JSON file, Map normalized and original CSV column names, stream the data using PyArrow to avoid memory issues when working with large files, Create data chunks and upload them to the oeplatform.
- `oep_upload.api` Using HTTP requests to: Create tables, post your data in chunks, upload metadata, get info on tables which already exists and create datapackages on the oeplatform. This is mostly used as helper in other modules.

- `oep_upload.cofig` Module to load settings, setup you application to your needs, use environments which change the target of the API requests from localhost to openenergyplatform.org and more.
- `.env` File to set your credentials and security info

## Use case

### Setup tool & Create tables

- Add your secrets and other settings to the config files for setup:

1. create a .env file
2. modify the file settings YAML files in `config`directory if needed

- add your data: Use Frictionless datapackage structure with oemetadata descriptions. Make sure your metadata is valid or at least make sure each table was inferred from the data itself using omi infer method.

- provide data using CSV or Excel (including a Transformation step when using .xlsx) using the wide format with 1 parameter per cell and one datatype per column (columns might be empty which is okay).

- Only the "id" column can be the PrimaryKey for a table, if it does not exists its added automatically. Keep in mind that this overrides your intended definitions.

### Upload data

- Once everything data related from create table step is completed meaning tables exists on the OEP in the "model_draft" section and data is properly formatted as well as file paths are available in the oemetadata you can upload your data automatically. Keep in mind that special cases exists and data types might not be handled. A simple fix requests will in most situation resolve this or other issues quickly.
- Also make sure you properly provided you api credentials in the config files so you have permission to access the OEP-API.

## Usage

You are supposed to pip install (once available) / clone this repo and import its functionality or simply run the main.py file after you completed the setup steps to make all oep-access credentials and the dataset source as well as oemetadata available and readable by the tool.

If you wonder where to put your data you can find a "data" directory where you drop you unzipped folder. In most scenarios there should then be a directory with the dataset name or acronym. Then in that directory a datapackage.json (which is the oemetadata JSON file) and then a data directory (in case of multiple files) which can include further sub directories to organize the data must be present. In simple cases you might just put the data file next to the json file. Other resources are also allowed but until now they are more or less ignored by the functionality of this tool. To make the upload work it is important to use the wide format of data and comply with the OEP as described in the Setup steps. Here might be questions on how to do that, please just reach out via issue to discuss you case. The relative paths to each data file must be mentioned in the datapackage.json for each resource - this ensures the tool can create a table and then also find the data which is supposed to be uploaded to that specific table.

You can also set further settings in `config.settings.base.yaml` if things dont work as expected have a look there and configure the `data_dir` and `datapackage_file` variables. I admit setting the correct paths might still be challenging as you have to be careful with leading and trailing "/". Just reach out via Issue so i can assist with that until i made it more stable. 

Find a file called ".env.example", copy it and name it .env. In that file you find multiple variable where you can provide specific info. Please provide your credentials that can be used with the api endpoint you use. In any chase that will be the oeplatoform and there you can find your API token in your Profile settings. !!Don´t commit production credentials!! Production is our public OEP instance which is running at openenergyplatform.org.

Run the main.py file with an installed python environment. I suggest to first install the package manager ["uv"](https://docs.astral.sh/uv/) on you system and then use it to install this tool. After that you just run the script or import its functionality to create automation pipelines.

## Limitations & Disclaimer

This tool is experimental and for my personal use cases. Im confident that it will help you but it depends on doing some things correctly. Main requirement is to setup a proper frictionless datapackage and provide the datapackage.json in oemetadata flavour. Each documented resource must provide a relative path to the csv file and everything should be stored in a directory which is copied to in the data directory in this repository. Additionally currently only support data types which are mapped in [oem2orm.postgresql_types](https://github.com/OpenEnergyPlatform/oem2orm/blob/develop/oem2orm/postgresql_types.py) they can be extended if needed.

If you don´t have an oemetadata document and/or you want to automatically describe your data using tool as implemented (soon) in the `oep_upload.describe` module or already implemented in `omi` or `frictionless_py` you might find data types like "any". This often indicates that your data is not properly structured. We use the wide format for data and we have to make sure that per column there is only a single data type. In practice this indicates that you cannot have a column of name `value` with type `numeric` and then add values `0.1` and in the next row text like `same as above`. Things must be machine readable and ver precisely formatted and described.

The benefit of this tool is that once you did all that pre-processing work which includes finding your data sources, transforming data, running your model to generate other data, describing your data with metadata information for both technical and informative reason, then you don`t have to worry about how you can publish this data. Due to the well described data you can upload or extend your upload or upload a new version which uses the same data structure with different values in a reproducible way. Once configured reuse easily.

Please also note that the tool might change your table and column names if they don`t comply with the restriction implemented by the oeplatform web-api. The restriction are mandatory to enable the software to create your uploaded tables on the PostgreSQL database.

So far this tool was tested with GB´s of data not big data. The CSV engine is implemented using PyArrow which can handle large volumes of data. Keep in mind that the oep upload is done via the Internet using a REST-API which relies on HTTP 1.1. In my case study uploading 2GB of data (scalar and timeseries) into multiple Tables took about 30 minutes. There might be possibilities to enhance that using parallel requests still this is basically the bottle neck due to technology constrains.



## Install from source for development or local usage

Recommended: Install uv

Read up here <https://docs.astral.sh/uv/getting-started/installation/#__tabbed_1_2>. As always things are easy to setup on Linux systems. On windows you have to do some extra steps. While uv is a nice tool which helps a lot it still requires some understanding how to setup python applications. If you have major issues getting that done you can always reach out via Issue here in this repo.

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
