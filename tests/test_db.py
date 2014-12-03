#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappy'

import unittest

import MySQLdb as mdb

from mock import Mock
from tlinsights.constants import DB


class TestDB(unittest.TestCase):
    def setUp(self):
        try:

            cxn = mdb.connect(DB.DB_SERVER, DB.DB_USER, DB.DB_PASS, DB.DB_NAME, use_unicode=True, charset=DB.DB_CHARSET)

        except mdb.MySQLError, e:
            print "Error: {:d}{}".format(e.args[0], e.args[1])
            exit(e.args[0])


def tearDown(self):
    pass


if __name__ == '__main__':
    unittest.main()