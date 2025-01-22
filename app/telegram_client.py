from telethon import TelegramClient
from app.config import API_ID, API_HASH

telegram_client = TelegramClient('bot_session', API_ID, API_HASH)
