import json

def get_metadata(type:str) -> list:
    with open("data/participants.json", "r", encoding="utf-8") as f:
        data = json.load(f)
        return [x[type].lower() for x in data]

COUNTRIES = get_metadata("country")
CONTESTANTS = get_metadata("contestant")
SONGS = get_metadata("song")