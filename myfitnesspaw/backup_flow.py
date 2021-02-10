"""
MyFitnessPaw's Backup Processing Flow.

This module hosts the functions to create database backups for the MyFitnessPaw
database file. Currently the backups are stored on a Dropbox drive and the files
are rotated to not exceed a specific number.
"""
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
        return []  # nothing to delete
    cut_index = len(timestamps) - max_num_backups
    return [f"mfp_db_backup_{ts.strftime('%Y-%m-%d')}" for ts in timestamps[:cut_index]]


@task(name="Backup MFP Database to Dropbox")
def make_dropbox_backup(
    dbx_token: str,
    dbx_mfp_dir: str,
) -> dropbox.files.FileMetadata:
    """Upload a copy of the database file to the Dropbox backup location."""
    timestamp = datetime.now().strftime("%Y-%m-%d")
    source_path = mfp.DB_PATH
    dest_path = f"{dbx_mfp_dir}/mfp_db_backup_{timestamp}"
    dbx = dropbox.Dropbox(dbx_token)
    with open(source_path, "rb") as file:
        res = dbx.files_upload(file.read(), dest_path, mode=WriteMode.overwrite)
    return res


@task(name="List Available Files in Dropbox Backup Directory")
def dbx_list_available_backups(
    dbx_token: str,
    dbx_mfp_dir: str,
) -> List[str]:
    """Query the Dropbox backup directory for available backup files."""
    dbx = dropbox.Dropbox(dbx_token)
    res = dbx.files_list_folder(dbx_mfp_dir)
    return [f.name for f in res.entries]


@task(name="Apply Backup Rotation Scheme")
def apply_backup_rotation_scheme(
    dbx_token: str,
    dbx_mfp_dir: str,
    files_list: Sequence,
) -> List[Tuple[Any, Any]]:
    """Apply the current backup rotation scheme (FIFO) to a provided file list."""
    #  hardcoding it to keep only the most recent 5 for now in order to see how it works.
    files_to_delete = select_fifo_backups_to_delete(5, files_list)
    dbx = dropbox.Dropbox(dbx_token)
    if not files_to_delete:
        return []
    deleted = []
    for filename in files_to_delete:
        res = dbx.files_delete(f"{dbx_mfp_dir}/{filename}")
        deleted.append((res.name, res.content_hash))
    return deleted


def get_backup_flow() -> Flow:
    """Return a flow to automate backup process."""
    with Flow("MyFitnessPaw DB Backup") as flow:
        dbx_mfp_dir = prefect.config.myfitnesspaw.backup.dbx_backup_dir
        dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
        flow.run_config = mfp.get_local_run_config()
        backup_result = make_dropbox_backup(dbx_token, dbx_mfp_dir)  # noqa
        avail_backups = dbx_list_available_backups(dbx_token, dbx_mfp_dir)
        res = apply_backup_rotation_scheme(  # noqa
            dbx_token, dbx_mfp_dir, avail_backups
        )
    return flow
