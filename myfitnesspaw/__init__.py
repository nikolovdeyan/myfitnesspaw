import sys
from pathlib import Path

import prefect

root_dir = Path().absolute()
python_ver = f"{sys.version_info.major}.{sys.version_info.minor}"

sitepackages_dir = root_dir.joinpath(
    ".venv", "lib", f"python{python_ver}", "site-packages"
)

ROOT_DIR = str(root_dir)
PYTHONPATH = str(sitepackages_dir)
MFP_CONFIG_PATH = str(root_dir.joinpath("mfp_config.toml"))
TEMPLATES_DIR = str(root_dir.joinpath("templates"))


database_dir = root_dir.joinpath("database")
database_dir.mkdir(parents=True, exist_ok=True)
database_file = prefect.config.myfitnesspaw.mfp_db_file
database_path = database_dir.joinpath(database_file)
DB_PATH = str(database_path)


from .api import run_etl_flow  # noqa
