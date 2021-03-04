"""
MyFitnessPaw, a Myfitnesspal user data harvesting tool.
"""

import os
import sys
from pathlib import Path

project_root = Path().absolute()
MFP_CONFIG_PATH = str(project_root.joinpath("mfp_config.toml"))
ROOT_DIR = str(project_root)
os.environ["PREFECT__USER_CONFIG_PATH"] = MFP_CONFIG_PATH


python_ver = f"{sys.version_info.major}.{sys.version_info.minor}"
sitepackages_dir = project_root.joinpath(
    ".venv", "lib", f"python{python_ver}", "site-packages"
)
PYTHONPATH = str(sitepackages_dir)
TEMPLATES_DIR = str(project_root.joinpath("templates"))

import prefect  # noqa

database_dir = project_root.joinpath("database")
database_dir.mkdir(parents=True, exist_ok=True)
database_file = "mfp_db.sqlite"
database_path = database_dir.joinpath(database_file)
DB_PATH = str(database_path)

from .api import run_etl_flow  # noqa
