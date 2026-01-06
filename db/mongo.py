from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from config import settings

class Mongo:
    client: AsyncIOMotorClient | None = None
    db: AsyncIOMotorDatabase | None = None

mongo = Mongo()

async def connect_to_mongo() -> None:
    if not settings.MONGO_URI:
        raise RuntimeError("MONGO_URI no está configurada")

    mongo.client = AsyncIOMotorClient(
        settings.MONGO_URI,
        connect=False,
        serverSelectionTimeoutMS=8000,
        uuidRepresentation="standard",
    )
    mongo.db = mongo.client[settings.MONGO_DB]

    # Validación temprana
    await mongo.db.command("ping")

async def close_mongo_connection() -> None:
    if mongo.client:
        mongo.client.close()
        mongo.client = None
        mongo.db = None
