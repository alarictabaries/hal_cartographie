from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
col_notices = client.hal.notices

duplicates = col_notices.aggregate([
    {"$group": {"_id": "$name", "count": {"$sum": 1}}},
    {"$match": {"_id": {"$ne": None}, "count": {"$gt": 1}}},
    {"$project": {"docid": "$_id", "_id": 0}}
])

for duplicate in duplicates:
    print(duplicate)
