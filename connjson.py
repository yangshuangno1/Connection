import json

with open('chiphiez.json', encoding="utf8") as json_file:
    data = json.load(json_file)
print(data)