import json
import ast


f = open("not_imported_ids.txt", "r", encoding='utf-8')
ls = f.readlines()

docs = []

for l in ls:
    l = ast.literal_eval(l)
    docs.append(l)

print(docs)

ids = []
for doc in docs:
    ids.append(doc["docid"])

print(ids)
