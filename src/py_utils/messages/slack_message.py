from __future__ import annotations

import json
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

__all__ = ["SlackWebhookError", "SlackMessageBase"]


class SlackWebhookError(RuntimeError):
    """Raised when the Slack webhook call fails."""


class SlackMessageBase:
    """Simple base class for posting messages via Slack incoming webhooks."""

    def __init__(
        self,
        webhook_url: str,
        default_channel: Optional[str] = None,
        default_username: Optional[str] = None,
        timeout: float = 10.0,
    ) -> None:
        if not webhook_url:
            raise ValueError("webhook_url is required")

        self.webhook_url = webhook_url
        self.default_channel = default_channel
        self.default_username = default_username
        self.timeout = timeout

    def build_payload(self, text: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Compose the payload body. Subclasses can override for custom formatting.

        Args:
            text: The message text to send.
            extra: Additional fields to merge into the payload (attachments, blocks, etc.).
        """
        payload: Dict[str, Any] = {"text": text}
        if self.default_channel:
            payload["channel"] = self.default_channel
        if self.default_username:
            payload["username"] = self.default_username
        if extra:
            payload.update(extra)
        return payload

    def _post_payload(self, payload: Dict[str, Any]) -> str:
        body = json.dumps(payload).encode("utf-8")
        request = Request(
            self.webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=self.timeout) as response:
                status = getattr(response, "status", response.getcode())
                content = response.read().decode("utf-8", errors="replace")
                if status and status >= 400:
                    raise SlackWebhookError(f"Slack webhook failed with status {status}: {content}")
                return content or "ok"
        except HTTPError as exc:
            content = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            raise SlackWebhookError(f"Slack webhook returned HTTP {exc.code}: {content or exc.reason}") from exc
        except URLError as exc:
            raise SlackWebhookError(f"Slack webhook request failed: {exc.reason}") from exc

    def send(self, text: str, extra: Optional[Dict[str, Any]] = None) -> str:
        """Send a message using the webhook. Returns the response text."""
        payload = self.build_payload(text, extra)
        return self._post_payload(payload)
