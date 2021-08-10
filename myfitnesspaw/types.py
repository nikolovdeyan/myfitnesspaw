from dataclasses import dataclass


@dataclass
class Style:
    title_bg_color: str
    article_bg_color: str
    bg0: str
    bg1: str
    bg2: str
    fg0: str
    fg1: str
    fg2: str
    text0: str
    text1: str
    text2: str
    accent: str
    faded0: str
    faded1: str
    warning: str


class User:
    def __init__(self, username, email):
        self.username = username
        self.email = email


class ProgressReport:
    def __init__(
        self,
        user: User,
        report_data,
        report_style: str = "lisk",
        report_template: str = "mfp_progress.jinja2",
    ):
        self.user = user
        self.data = report_data
        style_dict = {
            "title_bg_color": "#fe8821",
            "article_bg_color": "#feecd3",
            "bg0": "#FEECD3",
            "bg1": "#FEDBAB",
            "bg2": "#FEDBAB",
            "fg0": "#FFB967",
            "fg1": "#FE8821",
            "fg2": "",
            "text0": "",
            "text1": "#3C3A41",
            "text2": "",
            "accent": "#21D8FF",
            "faded0": "#958476",
            "faded1": "#CCBBAD",
            "warning": "#FF3D14",
        }
        self.style = Style(**style_dict)
        self.template = report_template
