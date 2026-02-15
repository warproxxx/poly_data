"""Alert module — writes signals to alerts.log and stdout. Telegram TBD."""

import json
import os
import sys
from datetime import datetime, timezone

SIGNALS_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SIGNALS_DIR, "alerts.log")
CONFIG_FILE = os.path.join(SIGNALS_DIR, "config.json")


def _load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


def send_alert(message: str):
    """Write alert to log file and stdout."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {message}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

    # Telegram (future)
    cfg = _load_config()
    token = cfg.get("telegram_bot_token", "")
    chat_id = cfg.get("telegram_chat_id", "")
    if token and chat_id:
        _send_telegram(token, chat_id, message)


def _send_telegram(token: str, chat_id: str, text: str):
    """Send via Telegram Bot API."""
    import urllib.request
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "HTML"}).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print(f"Telegram send failed: {e}", file=sys.stderr)
