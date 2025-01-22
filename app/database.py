from motor.motor_asyncio import AsyncIOMotorClient
from decouple import config

MONGO_CONNECTION_STRING = config("MONGO_CONNECTION_STRING")
mongo_client = AsyncIOMotorClient(MONGO_CONNECTION_STRING)

db = mongo_client["telegram_data_backup"]
messages_collection = db["messages"]
groups_collection = db["groups"]
tags_collection = db["tags"]
categories_collection = db["categories"]
users_collection = db["users"]
