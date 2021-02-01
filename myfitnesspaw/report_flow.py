from pathlib import Path

import jinja2
from prefect import Flow, task
from prefect.run_configs import LocalRun
from prefect.tasks.notifications.email_task import EmailTask
from prefect.tasks.secrets import PrefectSecret


@task
def prepare_template(templates_dir):
    template_loader = jinja2.FileSystemLoader(searchpath=templates_dir)
    template_env = jinja2.Environment(loader=template_loader)
    message = template_env.get_template("mailreport_base.html").render()
    return message


@task
def mail_user(usermail, message):
    e = EmailTask(
        subject="Greetings from Lisko Reporter!",
        msg=message,
        email_from="Lisko Reporting Service",
        smtp_server="smtp.gmail.com",
        smtp_port=465,
        smtp_type="SSL",
    )
    e.run(email_to=usermail)


def get_mail_report(user):
    with Flow("MyFitnessPaw Email Report") as flow:
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
        usermail = PrefectSecret(f"MYFITNESSPAL_USERNAME_{user.upper()}")
        templates_dir = working_dir.joinpath("templates")
        message = prepare_template(str(templates_dir))
        r = mail_user(usermail, message)  # noqa

    return flow
