from decouple import config

API_ID = config("API_ID", cast=int)
API_HASH = config("API_HASH")
PHONE_NUMBER = config("PHONE_NUMBER")
MONGO_CONNECTION_STRING = config("MONGO_CONNECTION_STRING")
OPENAI_API_KEY = config("OPENAI_API_KEY")
BOT_TOKEN = config("BOT_TOKEN")
