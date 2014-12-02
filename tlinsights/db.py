# -*- coding: utf-8 -*-
__author__ = 'cappy'

from sys import exit
from constants import DB, TWITTER
from datetime import datetime
from twitter import Status, UserStatus

import MySQLdb as mdb

try:
    cxn = mdb.connect(DB.DB_SERVER, DB.DB_USER, DB.DB_PASS, DB.DB_NAME, use_unicode=True, charset=DB.DB_CHARSET)
except mdb.MySQLError, e:
    print "Error: {:d}{}".format(e.args[0], e.args[1])
    exit(e.args[0])

def query_decorator(func):
    def query_wrapper(name):
        try:
            func(name)
        except mdb.MySQLError, e:
            print "Error: {:d}{}".format(e.args[0], e.args[1])
            exit(e.args[0])
    return query_wrapper

@query_decorator
def get_tweet_by_id(id):
    global cxn
    cur = cxn.cursor()
    # TODO: sanitize this?
    q = "SELECT * from twposts WHERE tweetID = %s"
    cur.execute(q, (id,)) # has to be a tuple, thus the ending comma
    result = cur.fetchone()
    return result

@query_decorator
def save_tweet(tweet):
    pass

@query_decorator
def save_tweets(tweets):
    pass

@query_decorator
def save_user(twitter_user):
    global cxn

    with cxn:
        cur = cxn.cursor()
        # odd - the format string here is NOT REALLY A NORMAL FORMAT STRING. YOU MUST USE %s for
        # all fields.
        # See http://stackoverflow.com/questions/5785154/python-mysqldb-issues-typeerror-d-format-a-number-is-required-not-str
        q = "INSERT IGNORE INTO twusers(id, name, screen_name, profile_url, location, time_zone) " \
            "VALUES (%s, %s, %s, %s, %s, %s)"
        arg_tuple = (long(twitter_user['id']),
                     twitter_user['name'],
                     twitter_user['screen_name'],
                     twitter_user['url'],
                     twitter_user['location'],
                     twitter_user['time_zone'])
        cur.execute(q, arg_tuple)

@query_decorator
def save_gos_interaction(gos_dict):
    global cxn

    with cxn:
        cur = cxn.cursor()
        # odd - the format string here is NOT REALLY A NORMAL FORMAT STRING. YOU MUST USE %s for
        # all fields.
        # See http://stackoverflow.com/questions/5785154/python-mysqldb-issues-typeerror-d-format-a-number-is-required-not-str
        q = "INSERT IGNORE INTO twgos(user_tweet_id, user_tweet_url, user_tweet_date, " \
            "user_tweet_text, support_tweet_id, support_tweet_date, support_tweet_text, support_gos, gos_type) " \
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        arg_tuple = (long(gos_dict['PostId']),
                     gos_dict['Post'],
                     _convert_time_from_twitter_to_mysql(gos_dict['PostDate']),
                     gos_dict['PostMessage'],
                     long(gos_dict['ReplyPostId']),
                     _convert_time_from_twitter_to_mysql(gos_dict['ReplyDate']),
                     gos_dict['ReplyMessage'],
                     gos_dict['GOS'],
                     gos_dict['GOSType'])
        cur.execute(q, arg_tuple)

def _convert_time_from_twitter_to_mysql(str_twitter_date):
    dt = datetime.strptime(str_twitter_date, TWITTER.TWITTER_TIME_FORMAT)
    return datetime.strftime(dt, DB.DB_TIME_FORMAT)

if __name__ == '__main__':
    json_user = r"""{
   "lang":"en",
   "favorited":true,
   "truncated":false,
   "text":".@JudyatAVG: Fever Pitch: Live Final is inspiration to us all http://t.co/54zBwzzPPW - @AVGFree @AVGBusiness #AVG #thepitch14",
   "created_at":"Tue Oct 28 10:22:28 +0000 2014",
   "retweeted":false,
   "source":"<a href=\"https://dev.twitter.com/docs/tfw\" rel=\"nofollow\">Twitter for Websites</a>",
   "user":{
      "id":403394026,
      "profile_sidebar_fill_color":"DDEEF6",
      "profile_text_color":"333333",
      "followers_count":185,
      "location":"Global",
      "profile_background_color":"233A5C",
      "listed_count":3,
      "utc_offset":-28800,
      "statuses_count":1241,
      "description":"Follow us for news, insight, ideas and practical know-how for growing your MSP business",
      "friends_count":101,
      "profile_link_color":"0084B4",
      "profile_image_url":"https://pbs.twimg.com/profile_images/2319098754/zfwde97dby5193r1iyt8_normal.jpeg",
      "profile_banner_url":"https://pbs.twimg.com/profile_banners/403394026/1414157293",
      "profile_background_image_url":"http://pbs.twimg.com/profile_background_images/778165478/6dd52db7f8bfeb0646bf1f39bbfabad7.jpeg",
      "screen_name":"AVGBusiness",
      "lang":"en",
      "profile_background_tile":false,
      "favourites_count":1,
      "name":"AVG Business",
      "url":"http://t.co/PxBfV9h5ds",
      "created_at":"Wed Nov 02 12:51:23 +0000 2011",
      "time_zone":"Pacific Time (US & Canada)",
      "protected":false
   },
   "retweet_count":2,
   "id":527042709786079233,
   "favorite_count":1
}"""
    import json
    tweet = json.loads(json_user)
    save_user(tweet['user'])