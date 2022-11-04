#!/usr/bin/python
import traceback
import os.path, twitter, time, sys
from datetime import datetime
from dateutil.parser import parse


from mongo_connector import MongoLoader
import configfile as cnf


def parse_string_date(date):
    return parse(date) if type(date) == str else date

class Crawler:
    def __init__(self):
        self.logfile = "crawler_log.txt"
        self.loader = MongoLoader(destination="tweets")

        if not os.path.isfile(cnf.KEYCONFIG["file"]):
            raise Exception("Crawler: keywords file not found")

        with open(cnf.KEYCONFIG["file"], 'r') as f:
            self.keywords = [line.strip() for line in f.readlines()]

        if len(self.keywords) == 0:
            raise Exception("Crawler: Keywords are zero length.")

        self.users = set(self.loader.get_user_ids())
        self.user_buffer = []
        self.user_counter = 0
        self.tweet_buffer = []
        self.tweet_counter = 0

    def dump_data(self):
        if self.user_counter > 0:
            self.loader.store_users(self.user_buffer)
            self.user_buffer = []
            self.user_counter = 0
        if self.tweet_counter > 0:
            self.loader.store_tweets(self.tweet_buffer)
            self.tweet_buffer = []
            self.tweet_counter = 0

    def write_log(self, msg):
        f_out = open(self.logfile, "a+")
        f_out.write(f'{datetime.now()} msg: {msg}\n')
        f_out.close()

    def fix_tweet(self, tweet):
        if "created_at" in tweet:
            # parse created datetime if exists
            tweet["created_at"] = parse_string_date(tweet["created_at"])

        if "user" in tweet:
            tweet["user"]['created_at'] = parse_string_date(tweet["user"]['created_at'])
            user_object = tweet["user"]
            user_object["id"] = int(user_object["id"])
            tweet["user_id"] = user_object["id"]

            if user_object["id"] not in self.users:
                user_object["created_at"] = parse_string_date(user_object["created_at"])
                user_object["probe_at"] = datetime.now()
                self.users.add(int(tweet["user_id"]))
                self.user_buffer.append(user_object)
                self.user_counter += 1

        """Recursively check quoted status and retweeted_status of tweet object"""
        if "quoted_status" in tweet:
            tweet["quoted_status"] = self.fix_tweet(tweet["quoted_status"])
        if "retweeted_status" in tweet:
            tweet["retweeted_status"] = self.fix_tweet(tweet["retweeted_status"])
        return tweet

    def collect(self):
        try:
            # create authentication twitter.API
            api = twitter.Api(consumer_key=cnf.TWCONFIG["consumer_key"],
                              consumer_secret=cnf.TWCONFIG["consumer_secret"],
                              access_token_key=cnf.TWCONFIG["access_token_key"],
                              access_token_secret=cnf.TWCONFIG['access_token_secret'],
                              tweet_mode='extended')
            # get twitter stream
            search = api.GetStreamFilter(track=self.keywords)

            # for each tweet collected by stream filter insert them into mongoDB greek collection
            for tweet in search:
                self.tweet_buffer.append(self.fix_tweet(tweet))
                self.tweet_counter += 1
                if self.tweet_counter > 200 or self.user_counter > 200:
                    self.dump_data()

        except Exception as e:
            #stack = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            stack = e
            self.write_log(f'Error occured: {e} and stack: {stack}')
            time.sleep(60 * 30)

    def start(self):
        self.write_log('Crawler process started.')
        while (True):
            self.collect()
            self.write_log('Crawler process restarted.')
            time.sleep(8)


if __name__ =="__main__":
    instance = Crawler()
    instance.start()
