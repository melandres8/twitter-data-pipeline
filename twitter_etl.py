import json
import s3fs
import tweepy
import pandas as pd
from datetime import datetime


def run_twitter_etl():
    # SECRETS
    access_key = API_KEY
    access_secret = API_SECRET_KEY
    consumer_key = ACCESS_TOKEN
    consumer_secret = ACCESS_TOKEN_SECRET


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)


    # API object connection
    api = tweepy.API(auth)

    tweets = api.user_timeline(
        screen_name='@elonmusk',
        count=200,
        include_rts=False,
        tweet_mode='extended'
    )

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {
            "user": tweet.user.screen_name,
            "text": text,
            "favorite_count": tweet.favorite_count,
            "retweet_count": tweet.retweet_count,
            "created_at": tweet.created_at
        }

        tweet_list.append(refined_tweet)


    df = pd.DataFrame(tweet_list)
    df.to_csv("refined_tweets.csv")
