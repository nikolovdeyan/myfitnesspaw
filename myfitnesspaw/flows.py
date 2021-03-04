"""
myfitnesspaw.flows

This module contains the predefined Prefect Flows to use with MyFitnessPaw.
"""

import prefect
from prefect import Flow, unmapped
from prefect.core import Parameter
from prefect.tasks.secrets import PrefectSecret

from . import DB_PATH, sql, tasks


def get_etl_flow(user=None, flow_name=None):
    """
    Get an ETL flow to extract data from myfitnesspal into the local database.
    """

    if not user:
        raise ValueError("An user must be provided for the flow.")

    mfp_insertmany = tasks.SQLiteExecuteMany(db=DB_PATH, enforce_fk=True)
    flow_name = flow_name if flow_name else f"MyFitnessPaw ETL <{user.upper()}>"

    with Flow(name=flow_name) as etl_flow:
        from_date, to_date = tasks.prepare_extraction_start_end_dates(
            from_date_str=Parameter(name="from_date", default=None),
            to_date_str=Parameter(name="to_date", default=None),
        )
        measures = Parameter(name="measures", default=["Weight"])
        username = PrefectSecret(f"MYFITNESSPAL_USERNAME_{user.upper()}")
        password = PrefectSecret(f"MYFITNESSPAL_PASSWORD_{user.upper()}")
        db_exists = tasks.create_mfp_database()
        dates_to_extract = tasks.generate_dates_to_extract(from_date, to_date)
        extracted_days = tasks.get_myfitnesspal_day.map(
            date=dates_to_extract,
            username=unmapped(username),
            password=unmapped(password),
            measures=unmapped(measures),
        )
        serialized_extracted_days = tasks.serialize_myfitnesspal_days(extracted_days)
        mfp_existing_days = tasks.mfp_select_raw_days(
            username=username,
            dates=dates_to_extract,
            upstream_tasks=[db_exists],
        )
        serialized_days_to_process = tasks.filter_new_or_changed_records(
            extracted_records=serialized_extracted_days,
            local_records=mfp_existing_days,
        )
        rawdays_load_state = mfp_insertmany(
            query=sql.insert_or_replace_rawdaydata_record,
            data=serialized_days_to_process,
        )

        days_to_process = tasks.deserialize_records_to_process(
            serialized_days=serialized_days_to_process,
            upstream_tasks=[rawdays_load_state],
        )
        note_records = tasks.extract_notes(days_to_process)
        notes_load_state = mfp_insertmany(  # noqa
            query=sql.insert_notes,
            data=note_records,
        )

        water_records = tasks.extract_water(days_to_process)
        water_load_state = mfp_insertmany(  # noqa
            query=sql.insert_water,
            data=water_records,
        )

        goal_records = tasks.extract_goals(days_to_process)
        goals_load_state = mfp_insertmany(  # noqa
            query=sql.insert_goals,
            data=goal_records,
        )
        meals_to_process = tasks.extract_meals(days_to_process)
        meal_records = tasks.extract_meal_records(meals_to_process)
        meals_load_state = mfp_insertmany(
            query=sql.insert_meals,
            data=meal_records,
        )

        mealentry_records = tasks.extract_mealentries(meals_to_process)
        mealentries_load_state = mfp_insertmany(  # noqa
            query=sql.insert_mealentries,
            data=mealentry_records,
            upstream_tasks=[meals_load_state],
        )

        cardio_records = tasks.extract_cardio_exercises(days_to_process)
        cardio_load_state = mfp_insertmany(  # noqa
            query=sql.insert_cardioexercises,
            data=cardio_records,
        )

        strength_records = tasks.extract_strength_exercises(days_to_process)
        strength_load_state = mfp_insertmany(  # noqa
            query=sql.insert_strengthexercises,
            data=strength_records,
        )

        measurements_records = tasks.extract_measures(days_to_process)
        measurements_load_state = mfp_insertmany(  # noqa
            query=sql.insert_measurements,
            data=measurements_records,
        )
    return etl_flow


def get_report_flow(user=None, report_type=None, flow_name=None):
    """
    Get a report flow to send an email showing the user's progress.
    """
    if not user:
        raise ValueError("An user must be provided for the flow.")
    flow_name = (
        flow_name if flow_name else f"MyFitnessPaw Email Report <{user.upper()}>"
    )
    with Flow(name=flow_name) as report_flow:
        usermail = PrefectSecret(f"MYFITNESSPAL_USERNAME_{user.upper()}")
        report_data = tasks.prepare_report_data_for_user(user, usermail)
        report_style = tasks.prepare_report_style_for_user(user)
        report_message = tasks.render_html_email_report(
            template_name="mfp_base.jinja2",
            report_data=report_data,
            report_style=report_style,
        )
        t = tasks.save_email_report_locally(report_message)  # noqa
        r = tasks.send_email_report(usermail, report_message)  # noqa
    return report_flow


def get_backup_flow(flow_name=None):
    """
    Get a backup flow to upload the MyFitnesPaw database to a dropbox location.
    """

    flow_name = flow_name if flow_name else "MyFitnessPaw DB Backup"

    with Flow(flow_name) as backup_flow:
        dbx_mfp_dir = prefect.config.myfitnesspaw.backup.dbx_backup_dir
        dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
        backup_result = tasks.make_dropbox_backup(dbx_token, dbx_mfp_dir)  # noqa
        avail_backups = tasks.dbx_list_available_backups(dbx_token, dbx_mfp_dir)
        res = tasks.apply_backup_rotation_scheme(  # noqa
            dbx_token, dbx_mfp_dir, avail_backups
        )
    return backup_flow
