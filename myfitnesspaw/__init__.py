import sys
from pathlib import Path

import prefect
from prefect.run_configs import LocalRun

import myfitnesspaw
from myfitnesspaw import backup_flow as backup  # noqa
from myfitnesspaw import etl_flow as etl  # noqa
from myfitnesspaw import report_flow as report  # noqa
from myfitnesspaw import schedules  # noqa
from myfitnesspaw import sql  # noqa


def setup_database(root_dir):
    database_dir = root_dir.joinpath("database")
    database_dir.mkdir(parents=True, exist_ok=True)
    database_file = prefect.config.myfitnesspaw.mfp_db_file
    database_path = database_dir.joinpath(database_file)
    return database_path


def get_local_run_config() -> LocalRun:
    """Return a LocalRun configuration to attach to a flow."""
    return LocalRun(
        working_dir=myfitnesspaw.ROOT_DIR,
        env={
            "PREFECT__USER_CONFIG_PATH": myfitnesspaw.MFP_CONFIG_PATH,
            "PYTHONPATH": myfitnesspaw.PYTHONPATH,
        },
    )


root_dir = Path().absolute()
python_ver = f"{sys.version_info.major}.{sys.version_info.minor}"

sitepackages_dir = root_dir.joinpath(
    ".venv", "lib", f"python{python_ver}", "site-packages"
)

db_path = setup_database(root_dir)

myfitnesspaw.ROOT_DIR = str(root_dir)
myfitnesspaw.DB_PATH = str(db_path)
myfitnesspaw.PYTHONPATH = str(sitepackages_dir)
myfitnesspaw.MFP_CONFIG_PATH = str(root_dir.joinpath("mfp_config.toml"))
myfitnesspaw.TEMPLATES_DIR = str(root_dir.joinpath("templates"))


def print_paths():
    print(f"myfitnesspaw.ROOT_DIR:        {myfitnesspaw.ROOT_DIR}")
    print(f"myfitnesspaw.DB_PATH:         {myfitnesspaw.DB_PATH}")
    print(f"myfitnesspaw.PYTHONPATH:      {myfitnesspaw.PYTHONPATH}")
    print(f"myfitnesspaw.MFP_CONFIG_PATH: {myfitnesspaw.MFP_CONFIG_PATH}")
    print(f"myfitnesspaw.TEMPLATES_DIR:   {myfitnesspaw.TEMPLATES_DIR}")
