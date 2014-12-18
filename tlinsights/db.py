# -*- coding: utf-8 -*-
__author__ = 'cappy'

from sys import exit
from datetime import datetime
from functools import wraps
from twitter import Status
import logging
import MySQLdb as mdb
import MySQLdb.cursors as cursors

from constants import DB, TWITTER

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TLInsightsDB(object):

    def __init__(self, db_name=DB.DB_NAME):
        try:
            self.cxn = mdb.connect(DB.DB_SERVER, DB.DB_USER, DB.DB_PASS, db_name, use_unicode=True,
                                   charset=DB.DB_CHARSET, cursorclass=cursors.DictCursor)
            self.cur = self.cxn.cursor()
        except mdb.MySQLError as err:
            logger.error("Error: {:d}{}".format(err.args[0], err.args[1]))
            exit(err.args[0])

    def _query_decorator(func):
        @wraps(func)
        def query_wrapper(self, *args):
            try:
                func(self, *args)
            except mdb.MySQLError as e:
                logger.error("Error: {:d} {}".format(e.args[0], e.args[1]))
                logger.debug(self.cur._last_executed)
                exit(e.args[0])

        return query_wrapper

    @_query_decorator
    def get_tweet_by_id(self, tweet_id):
        with self.cxn:
            q = "SELECT * from twposts WHERE id = %s"
            self.cur.execute(q, (tweet_id,))  # has to be a tuple, thus the ending comma
            result = self.cur.fetchone()
        return result


    @_query_decorator
    def save_tweet(self, tweet):

        result = None

        self.save_user(tweet.GetUser())

        q = """INSERT INTO twposts (id,
                                    user_id,
                                    created_at,
                                    text,
                                    retweeted,
                                    retweet_count,
                                    favorited,
                                    favorite_count,
                                    in_reply_to_status_id,
                                    in_reply_to_user_id,
                                    in_reply_to_screen_name,
                                    lang,
                                    truncated,
                                    possibly_sensitive)
                    VALUES (%s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s)
                    ON DUPLICATE KEY UPDATE
                      text = VALUES(text),
                      retweeted = VALUES(retweeted),
                      retweet_count = VALUES(retweet_count),
                      favorited = VALUES(favorited),
                      favorite_count = VALUES(favorite_count),
                      possibly_sensitive = VALUES(possibly_sensitive);
                    """

        with self.cxn:
            arg_tuple = (
                tweet.id,
                tweet.user.id,
                self._convert_time_from_twitter_to_mysql(tweet.created_at),
                self._decode_if_string(tweet.text),
                tweet.retweeted,
                tweet.retweet_count,
                tweet.favorited,
                tweet.favorite_count,
                tweet.in_reply_to_status_id,
                tweet.in_reply_to_user_id,
                tweet.in_reply_to_screen_name,
                self._decode_if_string(tweet.lang),
                tweet.truncated,
                tweet.possibly_sensitive
            )

            result = self.cur.execute(q, arg_tuple)

        return result


    def save_tweets(self, tweets):
        for tw in tweets:
            self.save_tweet(tw)

    @_query_decorator
    def save_404_tweet(self,tweet):

        with self.cxn:
            q = "INSERT IGNORE INTO tw404posts(id) VALUES (%s)"
            self.cur.execute(q, (tweet['id'],))

    def save_404_tweets(self, tweets):
        for tw in tweets:
            self.save_404_tweet(tw)


    @_query_decorator
    def save_user(self, twitter_user):

        with self.cxn:

            q = """INSERT INTO twusers (
                                    id,
                                    name,
                                    created_at,
                                    screen_name,
                                    url,
                                    description,
                                    statuses_count,
                                    favourites_count,
                                    followers_count,
                                    friends_count,
                                    listed_count,
                                    location,
                                    time_zone,
                                    lang,
                                    protected,
                                    verified,
                                    geo_enabled
                                   )
                            VALUES (
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s)
                            ON DUPLICATE KEY UPDATE
                                    name = VALUES(name),
                                    created_at = VALUES(created_at),
                                    screen_name = VALUES(screen_name),
                                    url = VALUES(url),
                                    description = VALUES(description),
                                    statuses_count = VALUES(statuses_count),
                                    favourites_count = VALUES(favourites_count),
                                    followers_count = VALUES(followers_count),
                                    friends_count = VALUES(friends_count),
                                    listed_count = VALUES(listed_count),
                                    location = VALUES(location),
                                    time_zone = VALUES(time_zone),
                                    lang = VALUES(lang),
                                    protected = VALUES(protected),
                                    verified = VALUES(verified),
                                    geo_enabled = VALUES(geo_enabled);"""

            arg_tuple = (twitter_user.id,
                         self._decode_if_string(twitter_user.name),
                         self._convert_time_from_twitter_to_mysql(twitter_user.created_at),
                         self._decode_if_string(twitter_user.screen_name),
                         twitter_user.url,
                         self._decode_if_string(twitter_user.description),
                         twitter_user.statuses_count,
                         twitter_user.favourites_count,
                         twitter_user.followers_count,
                         twitter_user.friends_count,
                         twitter_user.listed_count,
                         self._decode_if_string(twitter_user.location),
                         self._decode_if_string(twitter_user.time_zone),
                         self._decode_if_string(twitter_user.lang),
                         twitter_user.protected,
                         twitter_user.verified,
                         twitter_user.geo_enabled)

            result = self.cur.execute(q, arg_tuple)

            return result
        
    @_query_decorator
    def save_user_from_dict(self, twitter_user):

        with self.cxn:

            q = """INSERT INTO twusers (
                                    id,
                                    name,
                                    created_at,
                                    screen_name,
                                    url,
                                    description,
                                    statuses_count,
                                    favourites_count,
                                    followers_count,
                                    friends_count,
                                    listed_count,
                                    location,
                                    time_zone,
                                    lang,
                                    protected,
                                    verified,
                                    geo_enabled
                                   )
                            VALUES (
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s)
                            ON DUPLICATE KEY UPDATE
                                    name = VALUES(name),
                                    created_at = VALUES(created_at),
                                    screen_name = VALUES(screen_name),
                                    url = VALUES(url),
                                    description = VALUES(description),
                                    statuses_count = VALUES(statuses_count),
                                    favourites_count = VALUES(favourites_count),
                                    followers_count = VALUES(followers_count),
                                    friends_count = VALUES(friends_count),
                                    listed_count = VALUES(listed_count),
                                    location = VALUES(location),
                                    time_zone = VALUES(time_zone),
                                    lang = VALUES(lang),
                                    protected = VALUES(protected),
                                    verified = VALUES(verified),
                                    geo_enabled = VALUES(geo_enabled);"""

            arg_tuple = (long(self._check_and_decode_val(twitter_user, 'id')),
                         self._check_and_decode_val(twitter_user, 'name'),
                         self._convert_time_from_twitter_to_mysql(
                             self._check_and_decode_val(twitter_user, 'created_at')),
                         self._check_and_decode_val(twitter_user, 'screen_name'),
                         self._check_and_decode_val(twitter_user, 'url'),
                         self._check_and_decode_val(twitter_user, 'description'),
                         self._check_and_decode_val(twitter_user, 'statuses_count'),
                         self._check_and_decode_val(twitter_user, 'favourites_count'),
                         self._check_and_decode_val(twitter_user, 'followers_count'),
                         self._check_and_decode_val(twitter_user, 'friends_count'),
                         self._check_and_decode_val(twitter_user, 'listed_count'),
                         self._check_and_decode_val(twitter_user, 'location'),
                         self._check_and_decode_val(twitter_user, 'time_zone'),
                         self._check_and_decode_val(twitter_user, 'lang'),
                         self._check_and_decode_val(twitter_user, 'protected'),
                         self._check_and_decode_val(twitter_user, 'verified'),
                         self._check_and_decode_val(twitter_user, 'geo_enabled'))

            result = self.cur.execute(q, arg_tuple)

            return result

    @classmethod
    def _check_and_decode_val(cls, d, k):
        v = d.get(k, None)
        result = cls._decode_if_string(v)
        return result

    @classmethod
    def _decode_if_string(cls, val):
        return val.decode("utf8") if isinstance(val, basestring) else val

    @_query_decorator
    def save_gos_interaction(self, gos_dict):

        with self.cxn:
            # odd - the format string here is NOT REALLY A NORMAL FORMAT STRING. YOU MUST USE %s for
            # all fields.
            # See http://stackoverflow.com/questions/5785154/python-mysqldb-issues-typeerror-d-format-a-number-is-required-not-str
            q = """INSERT IGNORE INTO twgos(user_tweet_id,
                                            user_tweet_url,
                                            user_tweet_date,
                                            user_tweet_text,
                                            support_tweet_id,
                                            support_tweet_date,
                                            support_tweet_text,
                                            support_gos,
                                            gos_type)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE ( support_gos = VALUES(support_gos));"""
            arg_tuple = (long(gos_dict['PostId']),
                         gos_dict['Post'],
                         self._convert_time_from_twitter_to_mysql(gos_dict['PostDate']),
                         gos_dict['PostMessage'],
                         long(gos_dict['ReplyPostId']),
                         self._convert_time_from_twitter_to_mysql(gos_dict['ReplyDate']),
                         gos_dict['ReplyMessage'],
                         gos_dict['GOS'],
                         gos_dict['GOSType'])
            result = self.cur.execute(q, arg_tuple)
            return result

    @classmethod
    def _convert_time_from_twitter_to_mysql(cls, str_twitter_date):
        # time data 'Wed Nov 02 12:51:23 +0000 2011' does not match format '%m/%d/%Y %I:%M:%S %p'
        dt = datetime.strptime(str_twitter_date, TWITTER.TWITTER_API_TIME_FORMAT)
        newdt = dt.strftime(DB.DB_TIME_FORMAT)
        return newdt


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

    tweet_json = json.loads(json_user)
    tweet = Status.NewFromJsonDict(tweet_json)
    db = TLInsightsDB(DB.DB_TEST_NAME)
    db.save_tweet(tweet)