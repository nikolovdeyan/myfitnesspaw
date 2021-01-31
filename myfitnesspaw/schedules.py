from prefect.schedules import CronSchedule

DEFAULT_DAILY = CronSchedule("0 6 * * *")
DEFAULT_EVERY_2ND_DAY = CronSchedule("0 6 */2 * *")
DEFAULT_WEEKLY = CronSchedule("0 6 * * 1")
