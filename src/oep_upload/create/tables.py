from pathlib import Path
from oem2orm import oep_oedialect_oem2orm as oem2orm

from oep_upload.config import get_settings, export_env_vars
import pathlib

from oep_upload.config.logging import setup_logging

loggi = setup_logging()

settings = get_settings()
export_env_vars(settings)


def create_tables_on_oedb(metadata_folder_name: Path):
    """
    Create tables on the OEP database based on metadata files.

    :param metadata_folder_name: Path to the folder containing metadata files.
                                    must be part of the current directory.
    """
    db = oem2orm.setup_db_connection(
        host=settings.api.local.host,
        user=settings.oep_user,
        token=settings.oep_api_token_local,
    )
    folder = pathlib.Path.cwd() / metadata_folder_name
    tables = oem2orm.collect_tables_from_oem_files(db, folder)
    oem2orm.create_tables(db, tables)

    # Upload metadata for single table
    # metadata = oem2orm.mdToDict(metadata_folder_name, "oed_example.json")
    # if metadata:
    #     oem2orm.api_updateMdOnTable(metadata)


if __name__ == "__main__":

    ROOT = Path(settings.paths.root).resolve()
    DATA_ROOT = (ROOT / settings.paths.data_dir).resolve()
    OEM_FILE = (
        (DATA_ROOT / settings.paths.datapackage_file).resolve()
        if settings.paths.datapackage_file
        else None
    )

    path = Path(DATA_ROOT)
    print(path)
    create_tables_on_oedb(path)
