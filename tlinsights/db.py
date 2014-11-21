# -*- coding: utf-8 -*-
__author__ = 'cappy'

from tlinsights.constants import *

import MySQLdb as mdb

cxn = mdb.connect(DB_SERVER, DB_USER, DB_PASS, DB_NAME)
