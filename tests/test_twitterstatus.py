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

    def test_get_tweet_by_id(self):
        ts = twitterstatus.TLTwitterStatus()
        tweet_id = 1234
        result = ts.get_tweet_by_id(tweet_id)
        self.assertIsNotNone(result, "tweet with id {:d} should be found".format(tweet_id))