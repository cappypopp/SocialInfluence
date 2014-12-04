#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappy'

from time import sleep
import avgsupporttwitter
import unittest

from tlinsights.constants import TWITTER

class TimerTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_timer_logic(self):
        sleep_time = 60  # sec
        interval_in_seconds = 5
        number_of_pings_expected = sleep_time / interval_in_seconds  # number of times we should see message

        for i in xrange(number_of_pings_expected):
            pct = float(interval_in_seconds * i)/sleep_time
            #sleep(interval_in_seconds)
            print "Percent done: {:.1%}".format(pct)

        self.assertEqual(number_of_pings_expected, i + 1)
