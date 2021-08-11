from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
from myfitnesspal.exercise import Exercise
from myfitnesspal.meal import Meal

available_styles = {
    "lisk": {
        "bg0": "#FEF1E2",
        "bg1": "#FEDBAB",
        "bg2": "#FEC478",
        "fg0": "#FE9923",
        "fg1": "#FE8821",
        "fg2": "#E5741A",
        "text0": "#827F85",
        "text1": "#57555C",
        "text2": "#3C3A41",
        "accent0": "#21D8FF",
        "accent1": "#185B66",
        "gray0": "#DCC09B",
        "gray1": "#9E8E7D",
        "warning": "#FF3D14",
        "error": "#FF0000",
    },
    "solarized": {
        "bg0": "#FDF6E3",
        "bg1": "#EEE8D5",
        "bg2": "#DBD3BB",
        "fg0": "#C2BBA5",
        "fg1": "#A8A28F",
        "fg2": "#8F8979",
        "text0": "#586E75",
        "text1": "#073642",
        "text2": "#002B36",
        "accent0": "#268BD2",
        "accent1": "#2AA198",
        "gray0": "#93A1A1",
        "gray1": "#657B83",
        "warning": "#CB4B16",
        "error": "#DC322F",
    },
}
available_styles["default"] = available_styles.get("solarized")


@dataclass
class MaterializedDay:
    """
    A class to hold the properties from myfitnesspal that we are working with.
    """

    username: str
    date: datetime.date
    meals: List[Meal]
    exercises: List[Exercise]
    goals: Dict[str, float]
    notes: Dict  # currently python-myfitnesspal only scrapes food notes
    water: float
    measurements: Dict[str, float]


@dataclass
class Style:
    bg0: str
    bg1: str
    bg2: str
    fg0: str
    fg1: str
    fg2: str
    text0: str
    text1: str
    text2: str
    accent0: str
    accent1: str
    gray0: str
    gray1: str
    warning: str
    error: str


class User:
    def __init__(self, username, email):
        self.username = username
        self.email = email


class ProgressReport:
    def __init__(
        self,
        user: User,
        report_data,
        report_style_name: str = "default",
        report_template_name: str = "mfp_progress_report.jinja2",
    ):
        self.user = user
        self.data = report_data.get("data_table", None)
        self.period_start_date = report_data.get("starting_date", None)
        self.current_day_number = self.data[-1][0]
        self.email_subject = (
            f"MyfitnessPaw Progress Report (Day {self.current_day_number})"
        )
        self.email_from = "Lisko Home Automation"
        self.email_to = user.email
        self.end_goal = report_data.get("end_goal", None)
        self.template = report_template_name
        self.num_rows_report_tbl = report_data.get("num_rows_report_tbl", 7)
        style_pallete = available_styles.get(report_style_name)
        self.style = Style(**style_pallete)
        self.attachments = [self._render_progress_bar_chart()]

    @property
    def period_start_date(self):
        return self._period_start_date

    @period_start_date.setter
    def period_start_date(self, value: str):
        self._period_start_date = datetime.strptime(value, "%Y-%m-%d")

    def get_template_data_dict(self):
        current_day_number = self.data[-1][0]  # first field in last table row
        title = f"MyFitnessPaw Progress Report (Day {current_day_number})"
        user = f"{self.user.username}".capitalize()
        today = datetime.now().strftime("%d %b %Y")
        generated_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        nutrition_tbl_header = [
            "day",
            "date",
            "cal target",
            "deficit target",
            "deficit actual",
            "running deficit",
        ]
        nutrition_tbl_data = self._prepare_nutrition_table()

        return {
            "title": title,
            "user": user,
            "today": today,
            "nutrition_tbl_header": nutrition_tbl_header,
            "nutrition_tbl_data": nutrition_tbl_data,
            "generated_ts": generated_ts,
        }

    def get_template_style_dict(self):
        return {
            "title_bg_color": self.style.fg1,
            "title_text_color": self.style.text2,
            "article_bg_color": self.style.bg0,
            "article_text_color": self.style.text2,
            "table_border_color": self.style.fg1,
            "table_bg_header": self.style.bg2,
            "table_bg_color1": self.style.bg0,
            "table_bg_color2": self.style.bg1,
            "table_text_color": self.style.text2,
            "footer_bg_color": self.style.text2,
            "footer_text_color": self.style.text0,
            "footer_link_color": self.style.accent0,
        }

    def _render_progress_bar_chart(self):
        nutrition_tbl_data = self._prepare_nutrition_table()
        yesterday_tbl_row = nutrition_tbl_data[-1]
        current_date = yesterday_tbl_row[1]
        deficit_actual = yesterday_tbl_row[4]
        deficit_accumulated = yesterday_tbl_row[5]

        if deficit_actual < 0:
            deficit_remaining = (
                self.end_goal - deficit_accumulated + abs(deficit_actual)
            )
            current_date_data = (
                (
                    deficit_accumulated - abs(deficit_actual),
                    abs(deficit_actual),
                    deficit_remaining + deficit_actual,
                ),
                "warning",
            )
        else:
            deficit_remaining = self.end_goal - deficit_accumulated - deficit_actual
            current_date_data = (
                (
                    deficit_accumulated,
                    deficit_actual,
                    deficit_remaining,
                ),
                "accent0",
            )

        chart_data = {current_date: current_date_data}
        color = list(chart_data.values())[0][1]
        vals = tuple(chart_data.values())[0][0]
        category_colors = [
            self.style.gray1,
            self.style.warning if color == "warning" else self.style.accent0,
            self.style.gray0,
        ]
        labels = list(chart_data.keys())
        data = np.array(list(vals))
        data_cum = data.cumsum()
        fig = plt.figure(figsize=(5.5, 0.7))
        ax = fig.add_subplot(111)

        fig.set_facecolor("#00000000")
        ax.set_axis_off()
        ax.set_ymargin(0.5)
        ax.set_xlim(0, np.sum(data, axis=0).max())
        goals_bar = ax.barh(  # noqa
            labels,
            width=data,
            left=data_cum[:] - data,
            color=category_colors,
        )

        our_dir = Path().absolute()
        chart_dir = our_dir.joinpath(Path("tmp"))
        chart_dir.mkdir(exist_ok=True)
        chart_file = chart_dir.joinpath(Path("temp.png"))

        plt.savefig(chart_file)
        return chart_file

    def _prepare_nutrition_table(self):
        yesterday_str = (date.today() - timedelta(days=1)).strftime("%d-%b-%Y")
        # row[4] is the deficit actual for yesterday
        # we skip days where actual deficit is NULL when we prepare the table
        report_window_data = [row for row in self.data if row[4] is not None]
        # if report starts from today or yesterday has no entered info, return None
        if not report_window_data or report_window_data[-1][1] != yesterday_str:
            return {}
        nutrition_tbl_data = report_window_data[(self.num_rows_report_tbl * -1) :]
        return nutrition_tbl_data
