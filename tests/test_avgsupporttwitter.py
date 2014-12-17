#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappy'

from time import sleep
import avgsupporttwitter
import unittest
import json
from pprint import pprint

from tlinsights.constants import TWITTER

class TimerTest(unittest.TestCase):

    twitter_data_dir = "twitter-gos-data"
    tweets_file = "../{}/tweets.json".format(twitter_data_dir)

    @classmethod
    def setUpClass(cls):
        cls._build_tweet_key_histograms()

    @classmethod
    def _build_tweet_key_histograms(cls):
        """
        tweet_histo:

        {u'coordinates': 128,
         u'created_at': 10515,
         u'favorite_count': 2205,
         u'favorited': 10515,
         u'geo': 128,
         u'id': 10515,
         u'in_reply_to_screen_name': 5708,
         u'in_reply_to_status_id': 4113,
         u'in_reply_to_user_id': 5708,
         u'lang': 10515,
         u'place': 187,
         u'possibly_sensitive': 90,
         u'retweet_count': 2905,
         u'retweeted': 10515,
         u'retweeted_status': 1058,
         u'scopes': 9,
         u'source': 10515,
         u'text': 10515,
         u'truncated': 10515,
         u'user': 10515}

         user_histo:

        {u'created_at': 10515,
         u'description': 9306,
         u'favourites_count': 9984,
         u'followers_count': 10431,
         u'friends_count': 10488,
         u'geo_enabled': 3201,
         u'id': 10515,
         u'lang': 10515,
         u'listed_count': 8880,
         u'location': 8740,
         u'name': 10515,
         u'profile_background_color': 10515,
         u'profile_background_image_url': 10515,
         u'profile_background_tile': 10515,
         u'profile_banner_url': 8687,
         u'profile_image_url': 10515,
         u'profile_link_color': 10515,
         u'profile_sidebar_fill_color': 10515,
         u'profile_text_color': 10515,
         u'protected': 10515,
         u'screen_name': 10515,
         u'statuses_count': 10515,
         u'time_zone': 8776,
         u'url': 7295,
         u'utc_offset': 7609,
         u'verified': 3232}
        :return:
        """
        with open(cls.tweets_file, "rb") as fp:
            tweets = json.load(fp, encoding="utf8")

        cls.tweet_count = len(tweets)
        cls.tweet_histo = {}
        cls.user_histo = {}
        for i in xrange(cls.tweet_count):
            for k1 in tweets[i].keys():
                cls.tweet_histo[k1] = cls.tweet_histo.get(k1, 0) + 1
            for k2 in tweets[i]['user'].keys():
                cls.user_histo[k2] = cls.user_histo.get(k2, 0) + 1

        pprint(cls.tweet_histo)
        pprint(cls.user_histo)

    def test_timer_logic(self):
        sleep_time = 60  # sec
        interval_in_seconds = 5
        number_of_pings_expected = sleep_time / interval_in_seconds  # number of times we should see message

        for i in xrange(number_of_pings_expected):
            pct = float(interval_in_seconds * i)/sleep_time
            #sleep(interval_in_seconds)
            print "Percent done: {:.1%}".format(pct)

        self.assertEqual(number_of_pings_expected, i + 1)

    def test_load_tweets_from_json(self):
        with open(self.tweets_file, "rb") as fp:
            tweets = json.load(fp, encoding='utf8')

        self.assertGreater(len(tweets), 0)

    def test_id_field_in_all_tweets(self):
        self.assertEqual(self.tweet_count, self.tweet_histo['id'])

    def test_createdat_field_in_all_tweets(self):
        self.assertEqual(self.tweet_count, self.tweet_histo['created_at'])

    def test_favorited_field_in_all_tweets(self):
        self.assertEqual(self.tweet_count, self.tweet_histo['favorited'])

    def test_lang_field_in_all_tweets(self):
        self.assertEqual(self.tweet_count, self.tweet_histo['lang'])

    def test_retweeted_field_in_all_tweets(self):
        self.assertEqual(self.tweet_count, self.tweet_histo['retweeted'])

    def test_source_field_in_all_tweets(self):
        self.assertEqual(self.tweet_count, self.tweet_histo['source'])

    def test_text_field_in_all_tweets(self):
        self.assertEqual(self.tweet_count, self.tweet_histo['text'])

    def test_truncated_field_in_all_tweets(self):
        self.assertEqual(self.tweet_count, self.tweet_histo['truncated'])




