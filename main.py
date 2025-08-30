from pathlib import Path

from create.tables import create_tables_on_oedb

import logging
import logging.config
import yaml
from config import get_settings, export_env_vars


def setup_logging():
    try:
        with open("config/logging.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        logging.config.dictConfig(cfg)
    except Exception:
        logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    setup_logging()
    settings = get_settings()
    export_env_vars(settings)  # keeps compatibility with code using env vars
    logging.getLogger(__name__).info(
        "Starting with target=%s, base_url=%s",
        settings.api.target,
        settings.endpoint.api_base_url,
    )
    # ... rest of your program ...


path = Path("datapackages/example/")
create_tables_on_oedb(path)
