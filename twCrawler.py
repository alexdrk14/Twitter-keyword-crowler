#!/usr/bin/python

import twitter
import io 
import sys
import time
import datetime
import configfile as cnf
from pymongo import MongoClient
from dateutil.parser import parse
#configfile is file that contain all configuration fo twitterApi keys and tokes in TWCONFIG dict.
#--------- also contain configuration for database location/port db/collection in DBCONFIG dict
#--------- also KEYCONFIG contain keyword filename where all keywords/hashtags are separated by newline

def connect_to_db(setup):
  #connect to mongo db collection
  client = MongoClient(setup["address"], setup["port"])
  db = client[setup["db"]]
  collection = db[setup["collection"]]
  return client, db, collection

def read_keywords():
  #opening the hashtags in a sep file
  with open(cnf.KEYCONFIG["file"], 'r') as f:
    keywords=[line.rstrip() for line in f.readlines()]
  for key in keywords:
    key=key.decode('utf8')
  return keywords

def write_log_msg(logfile, msg):
  f_out = open(logfile, "a+")
  f_out.write(msg)
  f_out.flush()
  f_out.close()

def collect(logfile):
  #create authentication twitter.API
  api = twitter.Api(consumer_key=cnf.TWCONFIG["consumer_key"],
                  consumer_secret=cnf.TWCONFIG["consumer_secret"],
                  access_token_key=cnf.TWCONFIG["access_token_key"],
                  access_token_secret=cnf.TWCONFIG['access_token_secret'])
  #get twitter stream
  search=api.GetStreamFilter(track=read_keywords())

  client, db, collection = connect_to_db(cnf.DBCONFIG)
  #for each tweet collected by strem filter insert them into mongoDB greek collection
  try:
    for tweet in search:
      if "created_at" in tweet.keys():
        #parse created datetime if exists
        tweet["created_at"] = parse(tweet["created_at"])
      #insert each new tweet into collection
      collection.insert(tweet)
  except Exception as e:
    write_log_msg(logfile,"{} Error occured: {}.\n".format(datetime.datetime.now(),e))
    time.sleep(60*30)
  finally:
    client.close()


def main():
  logfile = "crawler_log.txt"
  write_log_msg(logfile,"{} Crawler process started.\n".format(datetime.datetime.now()))
  while(True):
    collect(logfile)
    write_log_msg(logfile,"{} Crawler process restarted.\n".format(datetime.datetime.now()))

if __name__ =="__main__": main()
