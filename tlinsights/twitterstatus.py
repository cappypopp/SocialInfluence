# -*- coding: utf-8 -*-
__author__ = 'cappy'

import twitter
import db
from constants import DB, TWITTER


class TLTwitterStatus(twitter.Status):

    db = db.TLInsightsDB(DB.DB_TEST_NAME)

    # need to call parent ctor?
    def __init__(self):
        pass

    @classmethod
    def get_tweet_by_id(cls, id):
        # get tweet as dictionary
        tweet_dict = cls.db.get_tweet_by_id(id)

        inst = None

        if tweet_dict is not None:
            # get user as dictionary
            twitter_user = TLTwitterUser.get_user_by_id(tweet_dict['user_id'])
            # build tweet instance
            inst = cls.NewFromJsonDict(tweet_dict)

            inst.SetUser(twitter_user)

        return inst


class TLTwitterUser(twitter.User):

    db = db.TLInsightsDB(DB.DB_TEST_NAME)

    def __init__(self):
        pass

    @classmethod
    def get_user_by_id(cls, id):
        # get tweet as dictionary
        user_dict = cls.db.get_user_by_id(id)

        inst = None

        if user_dict is not None:
            inst = cls.NewFromJsonDict(user_dict)

        return inst
