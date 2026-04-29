import requests
import logging
from typing import Dict, Any, Union


def send_webhook(webhook_url: str, message: Union[str, Dict[str, Any]], logger: logging.Logger) -> None:
    """
    Send a message or embed to a Discord webhook, preserving original formatting.

    :param webhook_url: The Discord webhook URL.
    :param message: Either a plain string (sent as a code block) or a dict representing a Discord embed.
    :param logger: Logger for error reporting.
    """
    headers = {"Content-Type": "application/json"}
    try:
        if isinstance(message, str):
            # Wrap the message in a code block to preserve formatting
            payload = {"content": f"```\n{message}\n```"}
            response = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
        else:
            # Send rich embed as-is
            payload = {"embeds": [message]}
            response = requests.post(webhook_url, json=payload, headers=headers, timeout=10)

        if response.status_code >= 400:
            logger.error(f"Webhook request failed ({response.status_code}): {response.text}")
    except Exception as e:
        logger.error(f"Error sending webhook: {e}")
