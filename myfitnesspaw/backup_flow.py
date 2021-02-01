from datetime import datetime
from typing import Any, List, Sequence, Tuple

import dropbox
import prefect
from dropbox.files import WriteMode
from prefect import Flow, task
from prefect.tasks.secrets import PrefectSecret

import myfitnesspaw as mfp


def select_fifo_backups_to_delete(
    max_num_backups: int, files_list: Sequence
) -> List[str]:
    """Return the oldest backups from a files_list to satisfy the max_num_backups."""
    timestamps = [datetime.strptime(f.split("_")[3], "%Y-%m-%d") for f in files_list]
    timestamps.sort()
    if len(timestamps) <= max_num_backups:
        return []
    cut_index = len(timestamps) - max_num_backups
    return [f"mfp_db_backup_{ts.strftime('%Y-%m-%d')}" for ts in timestamps[:cut_index]]


@task(name="Backup MFP Database to Dropbox")
def make_mfp_db_dropbox_backup() -> dropbox.files.FileMetadata:
    """Upload a copy of the database file to the Dropbox backup location."""
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    timestamp = datetime.now().strftime("%Y-%m-%d")
    source_path = prefect.config.myfitnesspaw.mfp_db_path
    dest_path = f"/myfitnesspaw/mfp_db_backups/mfp_db_backup_{timestamp}"
    dbx = dropbox.Dropbox(dbx_token.run())
    with open(source_path, "rb") as f:
        res = dbx.files_upload(f.read(), dest_path, mode=WriteMode.overwrite)
    return res


@task(name="List Available Files in Dropbox Backup Directory")
def dbx_list_available_backup_files() -> List[str]:
    """Query the Dropbox backup directory for available backup files."""
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    mfp_dir_path = "/myfitnesspaw/mfp_db_backups"
    dbx = dropbox.Dropbox(dbx_token.run())
    res = dbx.files_list_folder(mfp_dir_path)
    files_list = [f.name for f in res.entries]
    return files_list


@task(name="Apply Backup Rotation Scheme")
def apply_backup_rotation_scheme(files_list: Sequence) -> List[Tuple[Any, Any]]:
    """Apply the current backup rotation scheme (FIFO) to a provided file list."""
    logger = prefect.context.get("logger")
    #  hardcoding it to keep only the most recent 5 for now in order to see how it works.
    files_to_delete = select_fifo_backups_to_delete(5, files_list)
    if not files_to_delete:
        logger.info("No files deleted (maximum backups threshold not reached).")
        return []
    dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
    dbx_mfp_dir_path = "/myfitnesspaw/mfp_db_backups"

    dbx = dropbox.Dropbox(dbx_token.run())
    deleted = []
    for f in files_to_delete:
        res = dbx.files_delete(f"{dbx_mfp_dir_path}/{f}")
        deleted.append((res.name, res.content_hash))
    logger.info(f"Deleted {len(deleted)} file(s) according to backup rotation scheme.")
    return deleted


def get_backup_flow() -> Flow:
    """Return a flow to automate backup process."""
    with Flow("MyFitnessPaw DB Backup") as flow:
        flow.run_config = mfp.get_local_run_config()
        backup_result = make_mfp_db_dropbox_backup()  # noqa
        remote_backup_files = dbx_list_available_backup_files()
        res = apply_backup_rotation_scheme(remote_backup_files)  # noqa
    return flow
