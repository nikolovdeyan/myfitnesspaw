"""
This file contains the predefined Prefect Flows to use with MyFitnessPaw.
"""

import prefect
from prefect import Flow, flatten, mapped, unmapped
from prefect.core import Parameter
from prefect.tasks.secrets import PrefectSecret

from . import tasks


def get_etl_flow(user=None, flow_name=None):
    if not user:
        raise ValueError("An user must be provided for the flow.")
    flow_name = flow_name if flow_name else f"MyFitnessPaw ETL <{user.upper()}>"
    with Flow(name=flow_name) as etl_flow:
        from_date, to_date = tasks.parse_date_parameters(
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
        raw_days_load_state = tasks.mfp_insert_raw_days(serialized_days_to_process)
        days_to_process = tasks.deserialize_records_to_process(
            serialized_days=serialized_days_to_process,
            upstream_tasks=[raw_days_load_state],
        )
        notes_records = tasks.extract_notes_from_days(days_to_process)
        notes_load_state = tasks.mfp_insert_notes(notes_records)  # NOQA
        water_records = tasks.extract_water_from_days(days_to_process)
        water_load_state = tasks.mfp_insert_water(water_records)  # NOQA
        goals_records = tasks.extract_goals_from_days(days_to_process)
        goals_load_state = tasks.mfp_insert_goals(goals_records)  # NOQA
        meals_to_process = tasks.extract_meals_from_days(days_to_process)
        meals_records = tasks.extract_meal_records_from_meals(meals_to_process)
        mealentries_records = tasks.extract_mealentry_records_from_meals(
            meals_to_process
        )
        meals_load_state = tasks.mfp_insert_meals(meals_records)
        mealentries_load_state = tasks.mfp_insert_mealentries(  # NOQA
            mealentries_records, upstream_tasks=[meals_load_state]
        )
        cardio_exercises_to_process = tasks.extract_cardio_exercises_from_days(
            days_to_process
        )
        strength_exercises_to_process = tasks.extract_strength_exercises_from_days(
            days_to_process
        )
        cardio_exercises_load_state = tasks.mfp_insert_cardio_exercises(  # NOQA
            cardio_list=cardio_exercises_to_process,
        )
        strength_exercises_load_state = tasks.mfp_insert_strength_exercises(  # NOQA
            strength_list=strength_exercises_to_process,
        )
        measurements_records = tasks.get_myfitnesspal_measure(
            measure=mapped(measures),
            username=username,
            password=password,
            dates_to_extract=dates_to_extract,
        )
        measurements_load_state = tasks.mfp_insert_measurements(  # NOQA
            measurements=flatten(measurements_records),
            upstream_tasks=[db_exists],
        )
    return etl_flow


def get_report_flow(user=None, report_type=None, flow_name=None):
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
    flow_name = flow_name if flow_name else "MyFitnessPaw DB Backup"
    with Flow("MyFitnessPaw DB Backup") as backup_flow:
        dbx_mfp_dir = prefect.config.myfitnesspaw.backup.dbx_backup_dir
        dbx_token = PrefectSecret("MYFITNESSPAW_DROPBOX_ACCESS_TOKEN")
        backup_result = tasks.make_dropbox_backup(dbx_token, dbx_mfp_dir)  # noqa
        avail_backups = tasks.dbx_list_available_backups(dbx_token, dbx_mfp_dir)
        res = tasks.apply_backup_rotation_scheme(  # noqa
            dbx_token, dbx_mfp_dir, avail_backups
        )
    return backup_flow
