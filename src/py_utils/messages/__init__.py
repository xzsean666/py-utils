"""Message utilities (Slack, etc.)."""

from .slack_message import SlackMessageBase, SlackWebhookError

__all__ = ["SlackMessageBase", "SlackWebhookError"]
