"""Example script demonstrating SlackMessageBase.

Run with:
    SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... uv run tests/slack_message_example.py
"""

import os
from typing import Any, Dict, Optional

from py_utils import SlackMessageBase, SlackWebhookError


class DemoSlackMessage(SlackMessageBase):
    def build_payload(self, text: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # Add a prefix to make the example message stand out.
        prefixed_text = f"[py-utils demo] {text}"
        return super().build_payload(prefixed_text, extra)


def main() -> None:
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("Set SLACK_WEBHOOK_URL to run this example.")
        return

    slack = DemoSlackMessage(
        webhook_url=webhook_url,
        default_username="py-utils-bot",
    )

    try:
        response = slack.send(
            "Hello from py-utils!",
            extra={"icon_emoji": ":rocket:"},
        )
        print(f"Slack webhook response: {response}")
    except SlackWebhookError as exc:
        print(f"Slack webhook failed: {exc}")
        raise


if __name__ == "__main__":
    main()
