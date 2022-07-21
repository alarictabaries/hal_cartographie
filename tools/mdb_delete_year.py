from pymongo import MongoClient
from datetime import datetime

client = MongoClient('mongodb://localhost:27017/')
col_notices = client.hal.notices

col_notices.delete_many({"submittedDate_tdate":
    {
        "$gte": datetime(year=2020, month=3, day=1),
        "$lte": datetime(year=2021, month=1, day=1),
    }
})
