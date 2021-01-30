from datetime import datetime
from pathlib import Path

import dropbox
import prefect
from prefect import Flow, task
from prefect.run_configs import LocalRun
from prefect.schedules import CronSchedule
from prefect.tasks.secrets import PrefectSecret


def select_fifo_backups_to_delete(max_num_backups, files_list):
    timestamps = [datetime.strptime(f.split("_")[2], "%Y-%m-%d") for f in files_list]
    timestamps.sort()
    if len(timestamps) <= max_num_backups:
        return []
    cut_index = len(timestamps) - max_num_backups
    return [f"mfpdb_backup_{ts.strftime('%Y-%m-%d')}" for ts in timestamps[:cut_index]]


@task
def mfpdb_dropbox_backups_cleanup(files_list):
    logger = prefect.context.get("logger")
    files_to_delete = select_fifo_backups_to_delete(5, files_list)
    if not files_to_delete:
        logger.info("Maximum number of backups not reached. No files deleted.")
        return
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    mfp_dir_path = "/myfitnesspaw/mfpdb_backups"

    dbx = dropbox.Dropbox(dbx_token.run())
    deleted = []
    for f in files_to_delete:
        res = dbx.files_delete(f"{mfp_dir_path}/{f}")
        deleted.append((res.name, res.content_hash))
    logger.info(deleted)
    return deleted


@task
def list_mfpdb_dropbox_dir():
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    mfp_dir_path = "/myfitnesspaw/mfpdb_backups"
    dbx = dropbox.Dropbox(dbx_token.run())
    res = dbx.files_list_folder(mfp_dir_path)
    files_list = [f.name for f in res.entries]
    return files_list


@task(name="Backup MFP Database to Dropbox")
def make_mfpdb_dropbox_backup():
    """Upload a database copy to a dropbox location."""
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    timestamp = datetime.now().strftime("%Y-%m-%d")
    source_path = prefect.config.myfitnesspaw.mfpdb_path
    dest_path = f"/myfitnesspaw/mfpdb_backups/mfpdb_backup_{timestamp}"

    dbx = dropbox.Dropbox(dbx_token.run())
    with open(source_path, "rb") as f:
        res = dbx.files_upload(f.read(), dest_path)
    return res


daily_schedule = CronSchedule("0 6 * * *")
every_2nd_day_schedule = CronSchedule("0 6 */2 * *")
weekly_schedule = CronSchedule("0 6 * * 1")


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
    bup_files_list = list_mfpdb_dropbox_dir()
    res = mfpdb_dropbox_backups_cleanup(bup_files_list)
