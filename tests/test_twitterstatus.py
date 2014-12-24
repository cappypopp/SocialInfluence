# -*- coding: utf-8 -*-
__author__ = 'cappy'

import unittest
import logging
import twitter

from tlinsights import twitterstatus

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class TestTwitterStatus(unittest.TestCase):

    def setUp(self):
        pass

    def test_db_connection(self):
        db = twitterstatus.TLTwitterStatus.db
        self.assertIsNotNone(db, "db should not be None")

    def test_get_tweet_by_id_bad_id(self):
        tweet_id = 1234
        result = twitterstatus.TLTwitterStatus.get_tweet_by_id(tweet_id)
        self.assertIsNone(result, "tweet with id {:d} should be found".format(tweet_id))

    def test_get_tweet_by_id_good_id(self):
        tweet_id = 527042709786079233
        result = twitterstatus.TLTwitterStatus.get_tweet_by_id(tweet_id)
        self.assertIsNotNone(result, "tweet with id {:d} should be found".format(tweet_id))

    def test_get_user_by_id_bad_id(self):
        tweet_id = 1234
        result = twitterstatus.TLTwitterUser.get_user_by_id(tweet_id)
        self.assertIsNone(result, "tweet with id {:d} should be found".format(tweet_id))

    def test_get_user_by_id_good_id(self):
        tweet_id = 403394026
        result = twitterstatus.TLTwitterUser.get_user_by_id(tweet_id)
        self.assertIsNotNone(result, "tweet with id {:d} should be found".format(tweet_id))