import sys
from pathlib import Path

import prefect
from prefect.run_configs import LocalRun

from . import backup_flow, etl_flow, report_flow, schedules, sql  # noqa


def setup_database(root_dir):
    database_dir = root_dir.joinpath("database")
    database_dir.mkdir(parents=True, exist_ok=True)
    database_file = prefect.config.myfitnesspaw.mfp_db_file
    database_path = database_dir.joinpath(database_file)
    return database_path


def get_local_run_config() -> LocalRun:
    """Return a LocalRun configuration to attach to a flow."""
    return LocalRun(
        working_dir=ROOT_DIR,
        env={
            "PREFECT__USER_CONFIG_PATH": MFP_CONFIG_PATH,
            "PYTHONPATH": PYTHONPATH,
        },
    )


root_dir = Path().absolute()
python_ver = f"{sys.version_info.major}.{sys.version_info.minor}"

sitepackages_dir = root_dir.joinpath(
    ".venv", "lib", f"python{python_ver}", "site-packages"
)

db_path = setup_database(root_dir)


def print_paths():
    print(f"myfitnesspaw.ROOT_DIR:        {ROOT_DIR}")
    print(f"myfitnesspaw.DB_PATH:         {DB_PATH}")
    print(f"myfitnesspaw.PYTHONPATH:      {PYTHONPATH}")
    print(f"myfitnesspaw.MFP_CONFIG_PATH: {MFP_CONFIG_PATH}")
    print(f"myfitnesspaw.TEMPLATES_DIR:   {TEMPLATES_DIR}")


ROOT_DIR = str(root_dir)
DB_PATH = str(db_path)
PYTHONPATH = str(sitepackages_dir)
MFP_CONFIG_PATH = str(root_dir.joinpath("mfp_config.toml"))
TEMPLATES_DIR = str(root_dir.joinpath("templates"))
