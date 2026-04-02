import os
import requests
import logging

class TelegramAdapter:
    def __init__(self):
        self.token = os.environ.get('TELEGRAM_BOT_TOKEN')
        self.chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        if not self.token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set in environment variables.")
        self.base_url = f"https://api.telegram.org/bot{self.token}"

    def send_message(self, text: str) -> dict:
        url = f"{self.base_url}/sendMessage"
        payload = {
            'chat_id': self.chat_id,
            'text': text
        }
        try:
            resp = requests.post(url, data=payload, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if data.get('ok'):
                return {
                    "ok": True,
                    "message_id": data['result']['message_id'],
                    "error": None
                }
            else:
                logging.error(f"Telegram send_message error: {data}")
                return {
                    "ok": False,
                    "message_id": None,
                    "error": str(data)
                }
        except Exception as e:
            logging.error(f"Telegram send_message exception: {e}")
            return {
                "ok": False,
                "message_id": None,
                "error": str(e)
            }

    def delete_message(self, message_id: int) -> bool:
        url = f"{self.base_url}/deleteMessage"
        payload = {
            'chat_id': self.chat_id,
            'message_id': message_id
        }
        try:
            resp = requests.post(url, data=payload, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            if data.get('ok'):
                return True
            else:
                logging.error(f"Telegram delete_message error: {data}")
                return False
        except Exception as e:
            logging.error(f"Telegram delete_message exception: {e}")
            return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    adapter = TelegramAdapter()
    result = adapter.send_message("Test message from TelegramAdapter.")
    print("Send result:", result)
    if result["ok"] and result["message_id"]:
        deleted = adapter.delete_message(result["message_id"])
        print("Delete result:", deleted)
