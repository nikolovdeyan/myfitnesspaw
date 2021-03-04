"""
myfitnesspaw.schedules

MyFitnessPaw's Prefect Schedules module.

This module contains a set of predefined Prefect schedules to use with its workflows.
Use this Cron calculator to understand and modify the CronSchedule definitions:
https://crontab.guru/
"""

from prefect.schedules import CronSchedule

EVERY_DAY = CronSchedule("0 6 * * *")
EVERY_2ND_DAY = CronSchedule("0 6 */2 * *")
EVERY_MONDAY = CronSchedule("0 6 * * 1")
EVERY_1ST_DAY_OF_MONTH = CronSchedule("0 6 1 * *")

ETL_DEFAULT = EVERY_DAY
WEEKLY_REPORT_DEFAULT = EVERY_MONDAY
MONTHLY_REPORT_DEFAULT = EVERY_1ST_DAY_OF_MONTH
BACKUP_DEFAULT = EVERY_2ND_DAY
