import os
import json
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("MONGO_DB", "pikcha")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def load_dir_to_collection(dirpath, coll_name, id_field=None):
    coll = db[coll_name]
    files = [f for f in os.listdir(dirpath) if f.endswith(".json")]
    print(f"📂 Loading {len(files)} files from {dirpath} -> collection '{coll_name}'")
    for fname in files:
        path = os.path.join(dirpath, fname)
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)
        if id_field and id_field in doc:
            coll.replace_one({id_field: doc[id_field]}, doc, upsert=True)
        else:
            coll.insert_one(doc)
    print(f"✅ Collection '{coll_name}' updated")

if __name__ == "__main__":
    base = os.path.join(os.getcwd(), "data")
    load_dir_to_collection(os.path.join(base, "stores"), "stores", id_field="store_id")
    load_dir_to_collection(os.path.join(base, "products"), "products", id_field="id")
    load_dir_to_collection(os.path.join(base, "customers"), "customers", id_field="customer_id")
    load_dir_to_collection(os.path.join(base, "purchases"), "purchases", id_field="purchase_id")
    print("🎉 All files loaded to MongoDB")
