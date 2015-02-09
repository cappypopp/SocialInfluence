# usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappy'

import logging
import logging.handlers
import os
import constants.APP as APP

class TLRotatingLogFileHandler(logging.handlers.RotatingFileHandler):

    def __init__(self, filename, mode='wb', encoding='utf-8', **kwargs):
        # create our own path to the log file that's dynamic based on the source
        if not os.path.exists(APP.LOGGING_PATH):
            os.mkdir(APP.LOGGING_PATH)

        super(TLRotatingLogFileHandler, self).__init__(os.path.join(APP.LOGGING_PATH, filename), mode, encoding, **kwargs)

class BetterRotatingFileHandler(logging.handlers.RotatingFileHandler):

    def _open(self):
        # Ensure the directory exists
        if not os.path.exists(APP.LOGGING_PATH):
            os.makedirs(APP.LOGGING_PATH)

        return logging.handlers.RotatingFileHandler._open(self)