from configparser import ConfigParser
from pymongo import MongoClient
import os

config = ConfigParser()
config_path = os.path.join(
    os.path.dirname(__file__),
    "data.ini"
)
config.read(config_path)


print("Creating database and collection")

client: MongoClient = MongoClient(
	config.get("mongo", "url"),
	username=config.get("mongo", "username"),
	password=config.get("mongo", "password")
)
print(f"Client connection: {client.admin.command("ping")}")

db: MongoClient = client[db_name := config.get("mongo", "database")]
collection = db[config.get("mongo", "collection")]

if collection.name not in db.list_collection_names():
	db.create_collection(collection.name, capped=False)
else:
	print(f"Collection '{collection.name}' already exists.")


if db_name not in client.list_database_names():
	raise Exception(f"Database '{db_name}' was not created.")

print(f"Database '{db_name}' created successfully!")
