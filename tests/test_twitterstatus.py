# -*- coding: utf-8 -*-
__author__ = 'cappy'

import unittest
import logging
import twitter
from functools import wraps

from tlinsights import twitterstatus, utils

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)  # should only be called once in entire app

class TestTwitterStatus(unittest.TestCase):

    def setUp(self):
        test_defaults = {
            'user_id_good': 403394026,
            'user_id_bad': -1,
            'tweet_id_good': 417762026186735616,
            'tweet_id_bad': 9999999999999999999
        }
        for(param, value) in test_defaults.iteritems():
            setattr(self, param, value)

    #@unittest.skip("")
    @utils.logged()
    def test_db_connection(self):
        db = twitterstatus.TLTwitterStatus.db
        self.assertIsNotNone(db, "db should not be None")

    #@unittest.skip("")
    @utils.logged()
    def test_create_twitterstatus_with_no_arguments(self):
        with self.assertRaises(TypeError):
            inst = twitterstatus.TLTwitterStatus()

    #@unittest.skip("")
    @utils.logged()
    def test_default_values_twitterstatus_with_dict_arg(self):
        inst = twitterstatus.TLTwitterStatus({})
        self.assertEqual(inst.unanswered, False)

    @utils.logged()
    def test_default_values_twitterstatus_with_twitter_status_arg(self):
        tweet_dummy = twitter.Status()
        inst = twitterstatus.TLTwitterStatus(tweet_dummy)
        self.assertIsNotNone(inst)

    @utils.logged()
    def test_call_attr_found_in_twitterstatus_not_wrapped_tweet_instance(self):
        tweet_dummy = twitter.Status()
        inst = twitterstatus.TLTwitterStatus(tweet_dummy)
        self.assertIs(False, inst.unanswered, "default value of .unanswered should be false")

    @utils.logged()
    def test_call_attr_found_in_twitterstatus_from_wrapped_tweet_instance(self):
        tweet_dummy = twitter.Status()
        inst = twitterstatus.TLTwitterStatus(tweet_dummy)
        self.assertIs(None, inst.id, "default value of .id should be 0 and should come from wrapped twitter.Status inst")

    @utils.logged()
    def test_get_tweet_by_id_bad_id(self):
        result = twitterstatus.TLTwitterStatus.get_tweet_by_id(self.tweet_id_bad)
        self.assertIsNone(result, "tweet with id {:d} should NOT be found".format(self.tweet_id_bad))

    @utils.logged()
    def test_get_tweet_by_id_good_id(self):
        result = twitterstatus.TLTwitterStatus.get_tweet_by_id(self.tweet_id_good)
        self.assertIsNotNone(result, "tweet with id {:d} should be found".format(self.tweet_id_good))

    @utils.logged()
    def test_created_at_for_excel(self):
        result = twitterstatus.TLTwitterStatus.get_tweet_by_id(self.tweet_id_good)
        datestr = result.created_at_for_excel()
        logging.debug(datestr)
        self.assertIsNotNone(datestr)
        self.assertIsInstance(datestr, unicode)

    @utils.logged()
    def test_str_method(self):
        inst = twitterstatus.TLTwitterStatus({})
        self.assertIsNotNone(inst)
        s = str(inst)
        logging.debug(s)
        self.assertIsNotNone(s)
        self.assertTrue(len(s) > 0)
        self.assertTrue("tweet_inst" in s)
        self.assertTrue("unanswered" in s)
        self.assertIsInstance(s, str)

    @utils.logged()
    def test_repr_method(self):
        inst = twitterstatus.TLTwitterStatus({})
        self.assertIsNotNone(inst)
        s = repr(inst)
        logging.debug(s)
        self.assertIsNotNone(s)
        self.assertTrue(len(s) > 0)
        self.assertTrue("tweet_inst" in s)
        self.assertTrue("unanswered" in s)
        self.assertIsInstance(s, str)

    @utils.logged()
    def test_unicode_method(self):
        inst = twitterstatus.TLTwitterStatus({})
        self.assertIsNotNone(inst)
        s = unicode(inst)
        logging.debug(s)
        self.assertIsNotNone(s)
        self.assertTrue(len(s) > 0)
        self.assertTrue("tweet_inst" in s)
        self.assertTrue("unanswered" in s)
        self.assertIsInstance(s, unicode)

    @utils.logged()
    def test_detailed_str_method(self):
        inst = twitterstatus.TLTwitterStatus.get_tweet_by_id(self.tweet_id_good)
        self.assertIsNotNone(inst)
        s = str(inst)
        logging.debug(s)
        self.assertIsNotNone(s)
        self.assertTrue(len(s) > 0)
        self.assertTrue("tweet_inst" in s)
        self.assertTrue("unanswered" in s)


    @utils.logged()
    def test_detailed_repr_method(self):
        inst = twitterstatus.TLTwitterStatus.get_tweet_by_id(self.tweet_id_good)
        self.assertIsNotNone(inst)
        s = repr(inst)
        logging.debug(s)
        self.assertIsNotNone(s)
        self.assertTrue(len(s) > 0)
        self.assertTrue("tweet_inst" in s)
        self.assertTrue("unanswered" in s)
        self.assertTrue("id" in s)

    @utils.logged()
    def test_url_empty(self):
        inst = twitterstatus.TLTwitterStatus({})
        self.assertIsNotNone(inst)
        s = inst.url()
        self.assertIsNone(s)

    @utils.logged()
    def test_url_not_empty(self):
        inst = twitterstatus.TLTwitterStatus.get_tweet_by_id(self.tweet_id_good)
        self.assertIsNotNone(inst)
        s = inst.url()
        logger.debug(s)
        self.assertIsNotNone(s)
        self.assertIsInstance(s, unicode)
        self.assertTrue(len(s) > len("http://twitter.com/1/status/1"))

    @utils.logged()
    def test_everything_stored_as_unicode(self):
        inst = twitterstatus.TLTwitterStatus.get_tweet_by_id(self.tweet_id_good)
        self.assertIsNotNone(inst)
        for k, v in inst.__dict__.iteritems():
            if isinstance(v, basestring):
                logger.debug("testing {}".format(v))
                self.assertIsInstance(v, unicode)
            if isinstance(v, twitter.Status):
                for v in inst.tweet_inst.__dict__.values():
                    if isinstance(v, basestring):
                        logger.debug("testing {}".format(v))
                        self.assertIsInstance(v, unicode)