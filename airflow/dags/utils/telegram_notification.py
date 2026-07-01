import requests
import logging

logger = logging.getLogger(__name__)

def send_telegram_message(message: str, bot_token: str, chat_id: str) -> None:
    """Send HTML-formatted text message to a Telegram channel/chat."""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Message successfully sent to Telegram")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {str(e)}")
        raise
