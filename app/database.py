from motor.motor_asyncio import AsyncIOMotorClient
from decouple import config

MONGO_CONNECTION_STRING = config("MONGO_CONNECTION_STRING")
# Disable SSL certificate verification by adding tlsAllowInvalidCertificates=True
mongo_client = AsyncIOMotorClient(MONGO_CONNECTION_STRING, tlsAllowInvalidCertificates=True)

db = mongo_client["telegram_data_backup"]
messages_collection = db.get_collection("messages")
groups_collection = db["groups"]
tags_collection = db["tags"]
categories_collection = db["categories"]
users_collection = db["users"]
alert_settings_collection = db["alert_settings"]
threat_library_collection = db["threat_library"]
