"""Add missing sample documents without replacing or deleting existing records."""
import asyncio
import os
import pathlib

from bson import json_util
from dotenv import load_dotenv
from pymongo import AsyncMongoClient

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent
load_dotenv(dotenv_path=PROJECT_ROOT / ".env", override=False)


async def seed_samples():
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB")
    if not mongo_uri or not mongo_db:
        raise RuntimeError("MONGO_URI and MONGO_DB must be configured")

    client = AsyncMongoClient(mongo_uri, tz_aware=True)
    try:
        database = client[mongo_db]
        options = json_util.JSONOptions(tz_aware=True)
        for collection_name, key_fields in (
            ("employees", ("emp_code",)),
            ("attendance_logs", ("emp_code", "date")),
        ):
            path = PROJECT_ROOT / "sample_data" / f"{collection_name}.json"
            documents = json_util.loads(path.read_text(), json_options=options)
            inserted = 0
            for document in documents:
                key = {field: document[field] for field in key_fields}
                result = await database[collection_name].update_one(
                    key, {"$setOnInsert": document}, upsert=True
                )
                inserted += result.upserted_id is not None
            print(f"added {inserted:>2} missing documents to '{collection_name}'")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(seed_samples())
