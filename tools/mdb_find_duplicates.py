from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
col_notices = client.hal.notices

duplicates = col_notices.aggregate([
    {"$group": {
        "_id": {"docid": "$docid"},
        "uniqueIds": {"$addToSet": "$_id"},
        "count": {"$sum": 1}
    }
    },
    {"$match": {
        "count": {"$gt": 1}
    }
    }
], allowDiskUse=True)

for duplicate in duplicates:
    print(duplicate)
