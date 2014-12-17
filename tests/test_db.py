#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappypopp'

import unittest
from tlinsights import db
import json
import logging
from tlinsights.constants import DB
from twitter import Status, User

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

json_tweets = r"""
[{
    "created_at": "Mon Jun 02 20:49:22 +0000 2014",
    "favorited": false,
    "id": 473567068478902272,
    "lang": "en",
    "retweeted": false,
    "source": "<a href=\"https://dev.twitter.com/docs/tfw\" rel=\"nofollow\">Twitter for Websites</a>",
    "text": "Thank you for installing AVG PC TuneUp! | AVG Worldwide http://t.co/KGml1tUVZm \u0639\u0628\u0631 @AVGFree",
    "truncated": false,
    "user": {
      "created_at": "Thu Sep 26 19:43:40 +0000 2013",
      "description": "\u0644\u0627 \u0639\u0632\u0629 \u0644\u0646\u0627 \u0625\u0644\u0627 \u0628\u0627\u0644\u0625\u0633\u0644\u0627\u0645 \u0646\u062d\u0646 \u0642\u0648\u0645\u064c \u0623\u0639\u0632\u0646\u0627 \u0627\u0644\u0644\u0647 \u0628\u0627\u0644\u0623\u0633\u0644\u0627\u0645 \u0641\u0645\u0647\u0645\u0627 \u0627\u0628\u062a\u063a\u064a\u0646\u0627 \u0627\u0644\u0639\u0632\u0629 \u0641\u064a \u063a\u064a\u0631\u0647 \u0623\u0630\u0644\u0646\u0627 \u0627\u0644\u0644\u0647",
      "favourites_count": 9,
      "followers_count": 1444,
      "friends_count": 1318,
      "id": 1908968400,
      "lang": "ar",
      "listed_count": 2,
      "name": "ashrafalmolla",
      "profile_background_color": "352726",
      "profile_background_image_url": "http://abs.twimg.com/images/themes/theme5/bg.gif",
      "profile_background_tile": false,
      "profile_banner_url": "https://pbs.twimg.com/profile_banners/1908968400/1406818466",
      "profile_image_url": "https://pbs.twimg.com/profile_images/494857942303113216/uuyAl0Vd_normal.jpeg",
      "profile_link_color": "D02B55",
      "profile_sidebar_fill_color": "99CC33",
      "profile_text_color": "3E4415",
      "protected": false,
      "screen_name": "ashrafalmolla",
      "statuses_count": 2689,
      "time_zone": "Cairo",
      "utc_offset": 7200
    }
  },
  {
    "created_at": "Wed Nov 05 23:04:05 +0000 2014",
    "favorited": false,
    "id": 530133478688321536,
    "in_reply_to_screen_name": "NdagiKakaMassa",
    "in_reply_to_status_id": 530114462238662656,
    "in_reply_to_user_id": 300554428,
    "lang": "en",
    "retweeted": false,
    "source": "<a href=\"http://twitter.com\" rel=\"nofollow\">Twitter Web Client</a>",
    "text": "@NdagiKakaMassa Thank you for your kind words, Mundi! :) ^AH",
    "truncated": false,
    "user": {
      "created_at": "Fri Jul 31 14:24:08 +0000 2009",
      "description": "The Official Twitter account for AVG Technologies. Our award winning products let you be yourself on #PCs #iPhone and #Android devices",
      "favourites_count": 344,
      "followers_count": 177854,
      "friends_count": 5638,
      "id": 61781392,
      "lang": "en",
      "listed_count": 1122,
      "location": "Download us FREE today:",
      "name": "AVGFree",
      "profile_background_color": "FFFFFF",
      "profile_background_image_url": "http://pbs.twimg.com/profile_background_images/455756505132699648/Y_haq_Yj.jpeg",
      "profile_background_tile": false,
      "profile_banner_url": "https://pbs.twimg.com/profile_banners/61781392/1415280736",
      "profile_image_url": "https://pbs.twimg.com/profile_images/510057645122678784/-vD1xsVg_normal.png",
      "profile_link_color": "038543",
      "profile_sidebar_fill_color": "F6F6F6",
      "profile_text_color": "333333",
      "protected": false,
      "screen_name": "AVGFree",
      "statuses_count": 11201,
      "time_zone": "Pacific Time (US & Canada)",
      "url": "http://t.co/M5n5lfrb5J",
      "utc_offset": -28800,
      "verified": true
    }
  },
  {
    "created_at": "Tue Jun 10 16:34:11 +0000 2014",
    "favorited": false,
    "id": 476401952759087105,
    "in_reply_to_screen_name": "AVGFree",
    "in_reply_to_user_id": 61781392,
    "lang": "es",
    "retweeted": false,
    "source": "<a href=\"http://twitter.com\" rel=\"nofollow\">Twitter Web Client</a>",
    "text": "@AVGFree , C\u00f3mo puedo solicitar un reembolso de un producto AVG?",
    "truncated": false,
    "user": {
      "created_at": "Wed May 11 16:44:24 +0000 2011",
      "description": "I like listening to music all time!!! It\u00b4s my life...",
      "favourites_count": 441,
      "followers_count": 21,
      "friends_count": 125,
      "geo_enabled": true,
      "id": 296928842,
      "lang": "es",
      "listed_count": 1,
      "location": "Mexico City, Mexico",
      "name": "Iv\u00e1n Israel Gonz\u00e1lez",
      "profile_background_color": "022330",
      "profile_background_image_url": "http://abs.twimg.com/images/themes/theme15/bg.png",
      "profile_background_tile": false,
      "profile_banner_url": "https://pbs.twimg.com/profile_banners/296928842/1413148851",
      "profile_image_url": "https://pbs.twimg.com/profile_images/500730083347529729/w-QzmKll_normal.jpeg",
      "profile_link_color": "0084B4",
      "profile_sidebar_fill_color": "C0DFEC",
      "profile_text_color": "333333",
      "protected": false,
      "screen_name": "indieivanrock",
      "statuses_count": 318,
      "time_zone": "Mexico City",
      "url": "https://t.co/bD0dVmCvZo",
      "utc_offset": -21600
    }
  }]
"""


class TestDB(unittest.TestCase):

    def setUp(self):
        self.DB = db.TLInsightsDB(DB.DB_TEST_NAME)
        self.tweets = json.loads(json_tweets, "utf-8")

    def test_save_tweet(self):
        tweet = Status.NewFromJsonDict(self.tweets[0])
        result = self.DB.save_tweet(tweet)
        self.assertIsNotNone(result)

    def test_get_tweet_by_id_with_bad_id(self):
        id = 1231542323423
        with self.assertRaises(db.TweetNotFoundException):
            result = self.DB.get_tweet_by_id(id)
            logger.error(result)

    def test_get_twitter_user_by_id_with_bad_id(self):
        id = 123123123123
        with self.assertRaises(db.UserNotFoundException):
            result = self.DB.get_user_by_id(id)
            logger.error(result)

    def test_get_twitter_user_by_id_with_good_id(self):
        id = 403394026
        result = self.DB.get_user_by_id(id)
        self.assertIsNotNone(result, "User should not be None")

    def test_get_twitter_user_by_id_with_good_id_verify_id(self):
        id = 403394026
        result = self.DB.get_user_by_id(id)
        self.assertEqual(id, result['id'])

if __name__ == '__main__':
    unittest.main()