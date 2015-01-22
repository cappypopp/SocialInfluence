#!usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappy'

LOGGING_LOGFILE = u"./logs/output.log"
LOGGING_LOGFILE_FORMAT = u"%(asctime)s [%(lineno)d]%(name)-12s %(levelname)-8s %(message)s'"
LOGGING_CONSOLE_FORMAT = u"%(name)s[%(lineno)d]: %(message)s"

DATA_DIR = u"twitter-gos-data"
TWEETS_FILE = u"{}/tweets.json".format(DATA_DIR)
TWEETS_FILE_BACKUP = u"".join((TWEETS_FILE, ".bak"))
DEAD_TWEETS_FILE = u"{}/dead_tweets.json".format(DATA_DIR)

FIRST_TOUCH_FILE = DATA_DIR + u"/TW-First Touch Data.json"
SUPPORT_FILE = DATA_DIR + u"/TW-Support First Touch Data.json"
UNANSWERED_FILE = DATA_DIR + u"/TW-Unanswered.json"


