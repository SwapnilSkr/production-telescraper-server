from telethon import TelegramClient
from app.config import API_ID, API_HASH

# Initialize Telegram client
telegram_client = TelegramClient('session_name', API_ID, API_HASH)
