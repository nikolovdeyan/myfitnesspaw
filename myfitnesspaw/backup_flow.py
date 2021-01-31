from datetime import datetime
from pathlib import Path

import dropbox
import prefect
from dropbox.files import WriteMode
from prefect import Flow, task
from prefect.run_configs import LocalRun
from prefect.tasks.secrets import PrefectSecret


def select_fifo_backups_to_delete(max_num_backups, files_list):
    timestamps = [datetime.strptime(f.split("_")[3], "%Y-%m-%d") for f in files_list]
    timestamps.sort()
    if len(timestamps) <= max_num_backups:
        return []
    cut_index = len(timestamps) - max_num_backups
    return [f"mfp_db_backup_{ts.strftime('%Y-%m-%d')}" for ts in timestamps[:cut_index]]


@task(name="Backup MFP Database to Dropbox")
def make_mfp_db_dropbox_backup():
    """Upload a database copy to a dropbox location."""
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    timestamp = datetime.now().strftime("%Y-%m-%d")
    source_path = prefect.config.myfitnesspaw.mfp_db_path
    dest_path = f"/myfitnesspaw/mfp_db_backups/mfp_db_backup_{timestamp}"

    dbx = dropbox.Dropbox(dbx_token.run())
    with open(source_path, "rb") as f:
        res = dbx.files_upload(f.read(), dest_path, mode=WriteMode.overwrite)
    return res


@task(name="List Available Files in Dropbox Backup Directory")
def list_mfp_db_dropbox_dir():
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    mfp_dir_path = "/myfitnesspaw/mfp_db_backups"
    dbx = dropbox.Dropbox(dbx_token.run())
    res = dbx.files_list_folder(mfp_dir_path)
    files_list = [f.name for f in res.entries]
    return files_list


@task
def apply_backup_rotation_scheme(files_list):
    logger = prefect.context.get("logger")
    files_to_delete = select_fifo_backups_to_delete(5, files_list)
    if not files_to_delete:
        logger.info("No files deleted (maximum backups threshold not reached).")
        return
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    dbx_mfp_dir_path = "/myfitnesspaw/mfp_db_backups"

    dbx = dropbox.Dropbox(dbx_token.run())
    deleted = []
    for f in files_to_delete:
        res = dbx.files_delete(f"{dbx_mfp_dir_path}/{f}")
        deleted.append((res.name, res.content_hash))
    logger.info(f"Deleted {len(deleted)} file(s) according to backup rotation scheme.")
    return deleted


with Flow("MyFitnessPaw DB Backup") as flow:
    working_dir = Path().absolute()
    mfp_config_path = working_dir.joinpath("mfp_config.toml")
    pythonpath = working_dir.joinpath(".venv", "lib", "python3.9", "site-packages")
    flow.run_config = LocalRun(
        working_dir=str(working_dir),
        env={
            "PREFECT__USER_CONFIG_PATH": str(mfp_config_path),
            "PYTHONPATH": str(pythonpath),
        },
    )
    backup_result = make_mfp_db_dropbox_backup()
    bup_files_list = list_mfp_db_dropbox_dir()
    res = apply_backup_rotation_scheme(bup_files_list)
