import datetime
from datetime import timedelta

from prefect import Flow, Parameter, task
from prefect.tasks.notifications.email_task import EmailTask


@task
def select_report_data(username, from_date, to_date):
    pass


@task
def email_report(recepient_email, report_data):
    msg = None
    e = EmailTask(
        subject="MyFitnessPaw Stats Report",
        msg=msg,
        smtp_server="smtp.gmail.com",
        smtp_port=465,
        smtp_type="SSL",
    )
    e.run(email_to=recepient_email)


def create_email_report_flow():
    with Flow("MyFitnessPaw Email Report Flow") as flow:
        from_date = Parameter(  # noqa
            name="from_date",
            required=False,
            default=(datetime.date.today() - timedelta(days=7)).strftime("%Y/%m/%d"),
        )
        to_date = Parameter(  # noqa
            name="to_date",
            required=False,
            default=(datetime.date.today() - timedelta(days=1)).strftime("%Y/%m/%d"),
        )
    return flow
