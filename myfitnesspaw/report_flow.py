"""MyFitnessPaw's Reporting Flow.
This module currently hosts the composition of a single Prefect flow used to
prepare an email report with a set of statistics extracted from the MFP database,
and to send the email to the user registered with the flow.
"""
import datetime
import sqlite3
from contextlib import closing

import jinja2
from prefect import Flow, task
from prefect.tasks.notifications.email_task import EmailTask
from prefect.tasks.secrets import PrefectSecret

import myfitnesspaw as mfp


@task
def prepare_report_data_for_user(user, usermail: str) -> dict:
    """Return a dictionary containing the data to populate the experimental report."""
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.execute(mfp.sql.select_alpha_report_totals, (usermail,))
        report_totals = dict(c.fetchall())

        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        lastweek = yesterday - datetime.timedelta(days=6)
        c.execute(
            mfp.sql.select_alpha_report_range,
            (usermail, lastweek.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")),
        )
        report_week = dict(c.fetchall())

        mfp_report_data = {
            "title": f"MyFitnessPaw Scheduled Report for {user}",
            "user": f"{user}",
            "today": datetime.datetime.now().strftime("%d %b %Y"),
            "weekly": {
                "days_total": (
                    "Number of days scraped by MyFitnessPaw",
                    report_week.get("days_total"),
                ),
                "meals_consumed": (
                    "Days where at least one daily meal was tracked",
                    report_week.get("meals_consumed"),
                ),
                "meals_breakfast": (
                    "Days with breakfast",
                    report_week.get("meals_breakfast"),
                ),
                "meals_lunch": (
                    "Days with lunch",
                    report_week.get("meals_lunch"),
                ),
                "meals_dinner": (
                    "Days with dinner",
                    report_week.get("meals_dinner"),
                ),
                "meals_snacks": (
                    "Days with snacks",
                    report_week.get("meals_snacks"),
                ),
                "calories_consumed": (
                    "Total calories consumed (all time)",
                    report_week.get("calories_consumed"),
                ),
                "calories_breakfast": (
                    "Total calories consumed for breakfast (all time)",
                    report_week.get("calories_breakfast"),
                ),
                "calories_lunch": (
                    "Total calories consumed for lunch (all time)",
                    report_week.get("calories_lunch"),
                ),
                "calories_dinner": (
                    "Total calories consumed for dinner (all time)",
                    report_week.get("calories_dinner"),
                ),
                "calories_snacks": (
                    "Total calories consumed for snacks (all time)",
                    report_week.get("calories_snacks"),
                ),
            },
            "user_totals": {
                "days_with_meals": (
                    "Days with at least one meal tracked",
                    report_totals.get("days_with_meals"),
                ),
                "days_with_cardio": (
                    "Days with any cardio exercises tracked",
                    report_totals.get("days_with_cardio"),
                ),
                "days_with_strength": (
                    "Days with any strength exercises tracked",
                    report_totals.get("days_with_strength"),
                ),
                "days_with_measures": (
                    "Days with any measure tracked",
                    report_totals.get("days_with_measures"),
                ),
                "num_meals": (
                    "Total number of tracked meals",
                    report_totals.get("num_entries_meals"),
                ),
                "num_cardio": (
                    "Total number of cardio exercises",
                    report_totals.get("num_entries_meals"),
                ),
                "num_measures": (
                    "Total number of measures taken",
                    report_totals.get("num_entries_meals"),
                ),
                "calories_consumed": (
                    "Total number of calories consumed",
                    report_totals.get("total_calories_consumed"),
                ),
                "calories_exercised": (
                    "Total number of calories burned with exercise",
                    report_totals.get("total_calories_exercised"),
                ),
            },
            "footer": {
                "generated_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            },
        }
        return mfp_report_data


@task
def prepare_report_style_for_user(user: str) -> dict:
    """Return a dictionary containing the values to style the experimental report."""
    mfp_report_style = {
        "title_bg_color": "#fec478",
        "article_bg_color": "#EDF2FF",
    }
    return mfp_report_style


@task
def render_html_email_report(
    template_name: str, report_data: dict, report_style: dict
) -> str:
    """Render a Jinja2 HTML template containing the report to send."""
    template_loader = jinja2.FileSystemLoader(searchpath=mfp.TEMPLATES_DIR)
    template_env = jinja2.Environment(loader=template_loader)
    report_template = template_env.get_template(template_name)
    return report_template.render(data=report_data, style=report_style)


@task
def send_email_report(email_addr: str, message: str) -> None:
    """Send a prepared report to the provided address."""
    e = EmailTask(
        subject="MyFitnessPaw Report",
        msg=message,
        email_from="Lisko Reporting Service",
    )
    e.run(email_to=email_addr)


@task
def save_email_report_locally(message: str) -> None:
    "Temporary function to see the result of the render locally."
    with open("temp_report.html", "w") as f:
        f.write(message)


def get_report_flow_for_user(user: str) -> Flow:
    """Return a flow configured to send reports to user."""
    with Flow(f"MyFitnessPaw Email Report <{user.upper()}>") as flow:
        flow.run_config = mfp.get_local_run_config()
        usermail = PrefectSecret(f"MYFITNESSPAL_USERNAME_{user.upper()}")
        report_data = prepare_report_data_for_user(user, usermail)
        report_style = prepare_report_style_for_user(user)
        report_message = render_html_email_report(
            template_name="mfp_base.jinja2",
            report_data=report_data,
            report_style=report_style,
        )
        t = save_email_report_locally(report_message)  # noqa
        r = send_email_report(usermail, report_message)  # noqa
    return flow
