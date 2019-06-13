import json
import datetime
from nltk.sentiment.vader import SentimentIntensityAnalyzer


class TweetProcessor:

    def __init__(self, response_time=False):
        if response_time:
            self.waiting_response = {}
        self.response_time = response_time
        self.sentiment_analyzer = SentimentIntensityAnalyzer()

    def process_tweet(self, binary_data):
        json_data = json.loads(binary_data)
        data = self._filter_data(json_data)
        data["sentiment"] = self.sentiment_analyzer.polarity_scores(data["text"])["compound"]

        if self.response_time:
            data["response_time"] = None
            if data["user"]["id"] != 85741735:
                self.waiting_response[data["user"]["id"]] = data["created_at"]
            else:
                data["response_time"] = self._compute_response_time(data)
        return data

    @staticmethod
    def _filter_data(json_data):
        data = {"text": json_data["extended_tweet"]["full_text"] if json_data['truncated'] else json_data["text"],
                "created_at": int(datetime.datetime.strptime(json_data["created_at"], "%a %b %d %H:%M:%S %z %Y").timestamp()*1000),
                "in_reply_to_user_id": json_data["in_reply_to_user_id"],
                "in_reply_to_status_id": json_data["in_reply_to_status_id"],
                "source": json_data["source"],
                "coordinates": json_data["coordinates"]["coordinates"] if json_data["coordinates"] else None,
                "retweeted_status_id": json_data["retweeted_status"]["id"] if "retweeted_status" in json_data else None,
                "user": {}, "entities": {}}

        data["user"]["id"] = json_data["user"]["id"]
        data["user"]["name"] = json_data["user"]["name"]
        data["user"]["followers_count"] = json_data["user"]["followers_count"]
        data["user"]["verified"] = json_data["user"]["verified"]

        data["entities"]["urls"] = [url['expanded_url'] for url in json_data["entities"]["urls"]]
        data["entities"]["hashtags"] = [hashtag["text"] for hashtag in json_data["entities"]["hashtags"]]
        data["entities"]["user_mentions"] = [mention["id"] for mention in json_data["entities"]["user_mentions"]]

        return data

    def _compute_response_time(self, dict_data):
        response_timestamp = dict_data["created_at"]
        try:
            message_timestamp = self.waiting_response[dict_data["in_reply_to_user_id"]]
            response_time = int((response_timestamp - message_timestamp)/1000)/60
            del self.waiting_response[dict_data["in_reply_to_user_id"]]
        except KeyError:
            response_time = None
        return response_time
