import jinja2
from prefect import Flow, task
from prefect.tasks.notifications.email_task import EmailTask
from prefect.tasks.secrets import PrefectSecret

import myfitnesspaw as mfp


@task
def prepare_html_report(template_name: str, **kwargs) -> str:
    """Render a Jinja2 HTML template containing the report to send."""
    template_loader = jinja2.FileSystemLoader(searchpath=mfp.TEMPLATES_DIR)
    template_env = jinja2.Environment(loader=template_loader)
    message = template_env.get_template(template_name).render()
    return message


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
        message = prepare_html_report("mailreport_base.html")
        r = send_mail_report(usermail, message)  # noqa
    return flow
