__author__ = 'cappy'

import twitter

class Tweet():

    def __init__(self, tweet):

        self._id = tweet.GetId()
        self._text = tweet.GetText()
        self._user_id = tweet.GetUser().GetId()
        self._user_name = tweet.GetUser().GetName()
        self._created_at = tweet.GetCreatedAt()
        self._in_reply_to_id = tweet.GetInReplyToUserId()
        self._in_reply_to_name = tweet.GetInReplyToUserName()

    def __repr__(self):
        pass
    def __str__(self):
        pass