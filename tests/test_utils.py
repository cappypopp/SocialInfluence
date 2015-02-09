# usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappy'


import unittest
import datetime
import logging

from tlinsights import utils

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
    def test_excel_date_from_string(self):
        dt = datetime.datetime.now()
        ds = dt.strftime(utils.EXCEL_DATETIME_FORMAT_STRING)
        result = utils.excel_date_from_string(ds)
        logger.debug(result)
        self.assertIsNotNone(result, "should return valid number")
        self.assertTrue(result > 42000, "should be > 42K because all Excel dates are")

    @utils.logged()
    def test_excel_date_from_datetime(self):
        dt = datetime.datetime.now()
        result = utils.excel_date_from_datetime(dt)
        logger.debug(result)
        self.assertIsNotNone(result, "should return valid number")
        self.assertTrue(result > 42000, "should be > 42K because all Excel dates are")
