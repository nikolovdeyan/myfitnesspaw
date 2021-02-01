from pathlib import Path

import prefect
from prefect.run_configs import LocalRun

from myfitnesspaw import backup_flow as backup  # noqa
from myfitnesspaw import etl_flow as etl  # noqa
from myfitnesspaw import report_flow as report  # noqa
from myfitnesspaw import schedules  # noqa
from myfitnesspaw import sql  # noqa

with open(".python-version", "r") as f:
    pyversion = f.readline().rsplit(".", 1)[0]  # discard minor version
working_dir = Path().absolute()
db_path = working_dir.joinpath("database")
db_path.mkdir(parents=True, exist_ok=True)
db_file = prefect.config.myfitnesspaw.mfp_db_file


ROOT_DIR = str(working_dir)
DB_PATH = str(db_path.joinpath(db_file))
PYTHONPATH = str(
    working_dir.joinpath(".venv", "lib", f"python{pyversion}", "site-packages")
)
MFP_CONFIG_PATH = str(working_dir.joinpath("mfp_config.toml"))
TEMPLATES_DIR = str(working_dir.joinpath("templates"))


def get_local_run_config() -> LocalRun:
    """Return a LocalRun configuration to attach to a flow."""
    return LocalRun(
        working_dir=ROOT_DIR,
        env={
            "PREFECT__USER_CONFIG_PATH": MFP_CONFIG_PATH,
            "PYTHONPATH": PYTHONPATH,
        },
    )
