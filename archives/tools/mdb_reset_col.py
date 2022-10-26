from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
col_notices = client.hal.notices

col_notices.drop()