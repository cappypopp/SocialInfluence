# -*- coding: utf-8 -*-
__author__ = 'cappy'

import twitter
import db
from constants import DB, TWITTER


class TLTwitterStatus(twitter.Status):

    db = db.TLInsightsDB(DB.DB_TEST_NAME)

    def __init__(self):
        pass

    def get_tweet_by_id(self, id):
        result = self.db.get_tweet_by_id(id)
        return result
