from pathlib import Path

from oep_upload.config.logging import setup_logging
from oep_upload.create.tables import create_tables_on_oedb
from oep_upload.upload.datapackage import upload_tabular_data


from oep_upload.config import get_settings, export_env_vars


if __name__ == "__main__":
    loggi = setup_logging()
    settings = get_settings()
    export_env_vars(settings)  # keeps compatibility with code using env vars
    loggi.info(
        "Starting with target=%s, base_url=%s",
        settings.api.target,
        settings.endpoint.api_base_url,
    )
    # if you are confused how to set the path to the datapackage file you can override the settings here:
    # path = Path("datapackages/example/")
    
    # load the path from settings which have been configured by the user
    path = Path(settings.paths.data_dir)
    
    create_tables_on_oedb(path)
    upload_tabular_data()
