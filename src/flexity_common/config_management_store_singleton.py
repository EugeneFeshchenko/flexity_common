import json
import logging

from pymongo import MongoClient
from pymongo.database import Database
import pymongo


class ConfigManagementStoreSingleton:
    def __init__(self, SettingsConfigsClass, logger=None):
        self.logger = logger or logging.getLogger(__name__)
        self.config = SettingsConfigsClass().get("config-management-store")

        metadata_config = self.config.get("metadata-mongo-schema")
        if metadata_config:
            self.metadata_mongo_schema = json.load(open(metadata_config))
        self.client = self.create_client()
        self.db = self.client.get_database(self.config["database"])
        self.configurations_collection = self.db.get_collection(self.config["collection"])
        existing_index_names = set(self.configurations_collection.index_information())
        if '_id_' in existing_index_names:
            existing_index_names.remove('_id_')
        required_index_names = {'unique-app-name-type'}
        indexes_to_drop = existing_index_names.difference(required_index_names)
        indexes_to_create = required_index_names.difference(existing_index_names)
        for index_name_to_drop in indexes_to_drop:
            self.configurations_collection.drop_index(index_name_to_drop)
        index_models_to_create = []
        for index_name_to_create in indexes_to_create:
            if index_name_to_create == 'unique-app-name-type':
                index_models_to_create.append(pymongo.IndexModel(
                    [
                        ("application", pymongo.ASCENDING),
                        ("name", pymongo.ASCENDING),
                        ("type", pymongo.ASCENDING)
                    ],
                    unique=True,
                    name="unique-app-name-type"
                ))
            else:
                raise NotImplemented(f'Index for name {index_name_to_create} is not implemented')
        if index_models_to_create:
            created_index_names = self.configurations_collection.create_indexes(index_models_to_create)
            if not created_index_names:
                raise Exception("Failed create indexes for configurations collection")

    def create_validator(self):
        validator = {
            "$jsonSchema": {
                "bsonType": self.metadata_mongo_schema["bsonType"],
                "properties": self.metadata_mongo_schema["properties"],
                "required": self.metadata_mongo_schema["required"]
            }
        }
        return validator

    def create_client(self) -> MongoClient:
        if "auth" in self.config:
            if self.config["auth"] == "simple":
                connection_string = str.format(
                    "mongodb://{}:{}@{}:{}/",
                    self.config["user"],
                    self.config["password"],
                    self.config["host"],
                    self.config["port"])
                return MongoClient(connection_string, retryWrites=False)

            elif self.config["auth"] == "tls":
                raise Exception("not implemented")
            else:
                raise Exception(str.format("unsupported authentication type: {}", self.config["auth"]))
        else:
            raise Exception("DB authentication method not specified")

    def get_db(self) -> Database:
        return self.db

    def get_metadata_mongo_schema(self):
        return self.metadata_mongo_schema

    def close(self) -> None:
        self.client.close()

    def get_configurations_collection(self):
        return self.configurations_collection
