FLOW_ERROR = """
{
    "blocks": [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":rotating_light: Lisko Error Notification :rotating_light:",
                "emoji": true
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*A flow execution resulted in a failure. Please check the details below:*"
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "project:     `%s`"},
                {"type": "mrkdwn", "text": "flow: `%s`"}
            ]
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "message:  `%s`"}
        },
        {"type": "divider"},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Message delivered by :fox_face: *Lisko Home Automation* :fox_face:"
                }
            ]
        },
        {"type": "divider"}
    ]
}
"""
