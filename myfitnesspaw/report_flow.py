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
    with closing(sqlite3.connect(mfp.DB_PATH)) as conn, closing(conn.cursor()) as c:
        c.execute("PRAGMA foreign_keys = YES;")
        c.execute(mfp.sql.select_alpha_report_data, (usermail,))
        report_data = dict(c.fetchall())

    mfp_report_data = {
        "title": "MyFitnessPaw Scheduled Report for ",
        "user": user,
        "today": datetime.datetime.now().strftime("%d %b %Y"),
        # ----
        "scope_period": "N/A",
        "period_days_logged": "N/A",
        "period_meals": "N/A",
        "period_exercises": "N/A",
        "period_calories": "N/A",
        # ----
        "days_total": report_data.get("days_total"),
        "days_with_meals": report_data.get("days_with_meals"),
        "days_with_cardio": report_data.get("days_with_cardio"),
        "days_with_strength": report_data.get("days_with_strength"),
        "days_with_measures": report_data.get("days_with_measures"),
        # ---
        "num_entries_meals": report_data.get("num_entries_meals"),
        "num_entries_cardio": report_data.get("num_entries_cardio"),
        "num_entries_measures": report_data.get("num_entries_measures"),
        "total_calories_consumed": report_data.get("total_calories_consumed"),
        "total_calories_exercised": report_data.get("total_calories_exercised"),
    }
    return mfp_report_data


@task
def prepare_report_style_for_user(user: str) -> dict:
    mfp_report_style = {
        "title_bg_color": "#fec478",
        "article_bg_color": "#ffffff",
    }
    return mfp_report_style


@task
def render_html_mail_report(
    template_name: str, report_data: dict, report_style: dict
) -> str:
    """Render a Jinja2 HTML template containing the report to send."""
    template_loader = jinja2.FileSystemLoader(searchpath=mfp.TEMPLATES_DIR)
    template_env = jinja2.Environment(loader=template_loader)
    report_template = template_env.get_template(template_name)
    return report_template.render(data=report_data, style=report_style)


@task
def send_mail_report(email_addr: str, message: str) -> None:
    """Send a prepared report to the provided address."""
    e = EmailTask(
        subject="MyFitnessPaw Report",
        msg=message,
        email_from="Lisko Reporting Service",
    )
    e.run(email_to=email_addr)


def get_report_flow_for_user(user: str) -> Flow:
    """Return a flow configured to send reports to user."""
    with Flow(f"MyFitnessPaw Email Report <{user.upper()}>") as flow:
        flow.run_config = mfp.get_local_run_config()
        usermail = PrefectSecret(f"MYFITNESSPAL_USERNAME_{user.upper()}")
        report_data = prepare_report_data_for_user(user, usermail)
        report_style = prepare_report_style_for_user(user)
        report_message = render_html_mail_report(
            template_name="mfp_base.jinja2",
            report_data=report_data,
            report_style=report_style,
        )
        r = send_mail_report(usermail, report_message)  # noqa
    return flow
