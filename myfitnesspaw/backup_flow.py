import datetime
from pathlib import Path

import dropbox
import prefect
from prefect import Flow, task
from prefect.run_configs import LocalRun
from prefect.tasks.secrets import PrefectSecret


@task(name="Backup MFP Database to Dropbox")
def make_mfpdb_dropbox_backup():
    """Upload a database copy to a dropbox location."""
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
    source_path = prefect.config.myfitnesspaw.mfpdb_path
    dest_path = f"/myfitnesspaw/mfpdb_backups/mfpdb_backup_{timestamp}"

    dbx = dropbox.Dropbox(dbx_token.run())
    with open(source_path, "rb") as f:
        res = dbx.files_upload(f.read(), dest_path)
    return res


with Flow("MFP Database Backup to Dropbox") as flow:
    working_dir = Path().absolute()
    mfp_config_path = working_dir.joinpath("mfp_config.toml")
    pythonpath = working_dir.joinpath(".venv", "lib", "python3.9", "site-packages")
    flow.run_config = LocalRun(
        working_dir=str(working_dir),
        env={
            "PREFECT__USER_CONFIG_PATH": mfp_config_path,
            "PYTHONPATH": pythonpath,
        },
    )
    backup_result = make_mfpdb_dropbox_backup()
