import json
import datetime


def process(binary_data):
    json_data = json.loads(binary_data)
    data = filter_data(json_data)
    return data


def filter_data(json_data):
    data = {"text": "TEST ALEX <3", #json_data["extended_tweet"]["full_text"] if json_data['truncated'] else json_data["text"],
            "created_at": int(datetime.datetime.strptime(json_data["created_at"], "%a %b %d %H:%M:%S %z %Y").timestamp()*1000),
            "in_reply_to_user_id": json_data["in_reply_to_user_id"],
            "in_reply_to_status_id": json_data["in_reply_to_status_id"], "coordinates": json_data["coordinates"],
            "retweeted_status_id": json_data["retweeted_status"]["id"] if json_data["retweeted"] else None,
            "user": {}, "entities": {}}

    data["user"]["id"] = json_data["user"]["id"]
    data["user"]["name"] = json_data["user"]["name"]
    data["user"]["followers_count"] = json_data["user"]["followers_count"]
    data["user"]["verified"] = json_data["user"]["verified"]

    data["entities"]["urls"] = [url['expanded_url'] for url in json_data["entities"]["urls"]]
    data["entities"]["hashtags"] = [hashtag["text"] for hashtag in json_data["entities"]["hashtags"]]
    data["entities"]["user_mentions"] = [mention["id"] for mention in json_data["entities"]["user_mentions"]]

    return data
