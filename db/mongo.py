from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
import os

class Mongo:
    client: AsyncIOMotorClient | None = None
    db: AsyncIOMotorDatabase | None = None

mongo = Mongo()

async def connect_to_mongo() -> None:
    mongo_uri = os.getenv("MONGO_URI", "")
    mongo_db = os.getenv("MONGO_DB", "")

    if not mongo_uri:
        raise RuntimeError("MONGO_URI no está configurada")
    if not mongo_db:
        raise RuntimeError("MONGO_DB no está configurada")

    mongo.client = AsyncIOMotorClient(
        mongo_uri,
        connect=False,
        serverSelectionTimeoutMS=8000,
        uuidRepresentation="standard",
    )
    mongo.db = mongo.client[mongo_db]

    await mongo.db.command("ping")

async def close_mongo_connection() -> None:
    if mongo.client:
        mongo.client.close()
        mongo.client = None
        mongo.db = None
