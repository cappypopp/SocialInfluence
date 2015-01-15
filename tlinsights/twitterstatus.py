# -*- coding: utf-8 -*-
__author__ = 'cappy'

import twitter
import db
import logging
import simplejson
from dateutil.parser import *
from time import sleep
import constants


'''
LOGGING
'''
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
'''
CUSTOM EXCEPTIONS
'''
class TwitterUnrecoverableException(Exception):
    pass


class TwitterContinueProcessing(Exception):
    pass


class TLTwitterAPI(twitter.Api):

    api = None

    twitter_keys = constants.TWITTER.get_twitter_keys()

    keys = None

    def __init__(self, **kwargs):
        super(TLTwitterAPI, self).__init__(**kwargs)

    @classmethod
    def _do_api_sleep(cls, wait_time=constants.TWITTER.TWITTER_RATE_LIMIT_DELAY_IN_SECONDS):
        msg = "\nENTERING API RATE LIMIT SLEEP PHASE!\n"
        logger.info("{:*^120}".format(msg))
        number_of_seconds_between_heartbeats = 5
        ui_heartbeat_intervals = wait_time / number_of_seconds_between_heartbeats

        for i in xrange(ui_heartbeat_intervals):
            pct = float(number_of_seconds_between_heartbeats * i) / wait_time
            logger.info("{:.1%} time elapsed till next API window ...".format(pct))

            sleep(number_of_seconds_between_heartbeats)

    @classmethod
    def get_twitter_status_from_api(cls, tweet_id, db):

        tw = None

        try:

            if not cls.api:

                try:

                    logging.info("{p:*^32}\nFETCHING NEXT SET OF KEYS!\n{p:*^32}".format(p=''))

                    # fetch the next set of keys from the generator
                    keys = cls.twitter_keys.next()

                except StopIteration:

                    logging.error("{p:*^32}\nOUT OF API KEYS -- SLEEPING!\n{p:*^32}".format(p=''))
                    # we've reached the end of our collection of keys so we
                    # reset our generator to re-use it from the beginning
                    twitter_keys = constants.TWITTER.get_twitter_keys()

                    # fetch next set of keys for when we wake up
                    keys = twitter_keys.next()

                    # we've exhausted our keys, sleep is only option
                    cls._do_api_sleep()

                cls.api = twitter.Api(consumer_key=keys["consumer_key"],
                                      consumer_secret=keys["consumer_secret"],
                                      access_token_key=keys["access_key"],
                                      access_token_secret=keys["access_secret"])

            # get the next tweet via Twitter API,
            # remove all entities (pics, vids, etc.)
            tw = cls.api.GetStatus(tweet_id, include_entities=False)

            if tw is not None:
                # store the tweet in the cache so we won't have to look it up again
                #cache_tweet(tweets_to_cache, tw)
                db.save_tweet(tw)

        except twitter.TwitterError as te:

            code = te.args[0][0]["code"]
            logging.error(te)
            if constants.TWITTER.TWITTER_ERR_RATE_LIMIT_EXCEEDED == code:
                # wipe out the existing API connection, we'll use a new one with a different set of
                # keys if possible on the next iteration
                cls.api = None

                # remove the tweet we added to the already-seen cache so if we can handle and continue
                # from the exception we won't miss it
                #tweets_processed.remove(tweet_id)

                raise TwitterContinueProcessing("handled Twitter exception - continue processing - rate limit hit")

            if constants.TWITTER.TWITTER_ERR_TWITTER_OVER_CAPACITY == code:
                # wait for the amount of time the rate limiter suggests before continuing
                cls._do_api_sleep(constants.TWITTER.TWITTER_OVER_CAPACITY_DELAY_IN_SECONDS)

                raise TwitterContinueProcessing("handled Twitter exception - continue processing - over capacity")

            if constants.TWITTER.TWITTER_ERR_TWEET_NOT_FOUND == code or \
                    constants.TWITTER.TWITTER_ERR_USER_PROTECTED_TWEETS == code or \
                    constants.TWITTER.TWITTER.TWITTER_ERR_USER_SUSPENDED == code:
                # tweet not found - either the original user deleted it or twitter is not
                # providing it via the API. status code 179 means we're not authorized to view
                # that tweet, same diff
                #tweets_not_found_or_private.append(tweet_id)  # save the tweet in our cache of unreachable tweets
                db.save_404_tweet(tweet_id)
                #if parent_id:
                    # we are in the middle of conversation thread - clear the support tweets
                    # and move on with the next conversation. Also cache the tweet ID so we can track
                    # these and ignore in the future
                    #support_tweets = None
                logging.debug("[DEAD THREAD - REMOVING]")

                    # TODO: where do we go from here?

        return tw

class TLTwitterStatus(object):

    db = db.TLInsightsDB(constants.DB.DB_TEST_NAME)

    def __init__(self, tweet_data, **kwargs):
        if isinstance(tweet_data, twitter.Status):
            self.tweet_inst = tweet_data
        elif isinstance(tweet_data, dict):
            self.tweet_inst = twitter.Status.NewFromJsonDict(tweet_data)
        else:
            raise TypeError("wrong type passed to __init__: must be instance of twitter.Status or dict")

        param_defaults = {
            'unanswered': False
        }
        for(param, default) in param_defaults.iteritems():
            setattr(self, param, kwargs.get(param, default))

    def __getattr__(self, item):
        return getattr(self.tweet_inst, item)

    # need to call parent ctor?
    def __init__other(self, data=None, **kwargs):
        param_defaults = {
            'unanswered': False
        }

        for (param, default) in param_defaults.iteritems():
            setattr(self, param, kwargs.get(param, default))

        if data is None:
            # init base
            super(TLTwitterStatus, self).__init__(**kwargs)
        else:
            for(k, v) in data.iteritems():
                setattr(self, param, data.get(k, None))


    def url(self):
        url = None
        if self.tweet_inst.id is not None:
            url = "http://twitter.com/{}/status/{}".format(str(self.tweet_inst.user.id), str(self.tweet_inst.id))
        return url

    def created_at_for_excel(self):
        if isinstance(self.created_at, basestring):
            dt = parse(self.created_at)
        else:
            dt = self.created_at

        s = dt.strftime(constants.TWITTER.TWITTER_TIME_FORMAT)
        return s

    @classmethod
    def get_tweet_by_id(cls, tweet_id):

        tweet = None

        if cls.db.is_404_tweet(tweet_id):
            # if this tweet is one of the ones that twitter returns
            # a status 34 for (not found) then just skip it - not worth
            # wasting a valuable rate-limited API call on it again
            logging.info("DEAD TWEET WITH ID: {}".format(tweet_id))

        else:
            tweet_raw = cls.db.get_tweet_by_id_raw(tweet_id)

            if tweet_raw is not None:
                tweet = cls._build_tweet_from_db(tweet_raw)
            else:
                processing = True
                while processing:
                    try:
                        tweet = TLTwitterAPI.get_twitter_status_from_api(tweet_id, cls.db)
                        # wrap it
                        tweet = cls(tweet)
                        processing = False
                    except TwitterContinueProcessing as e:
                        logging.info(e)
                        processing = True

        return tweet

    @classmethod
    def save_tweet(cls, tweet):
        pass

    @classmethod
    def _build_tweet_from_db(cls, db_dict):
        # split off the twitter.Status keys into a twitter.Status-friendly dict
        tweet_dict = {k: v for k, v in db_dict.iteritems() if not k.startswith("user_")}

        # need to put in "Mon Jun 02 20:49:22 +0000 2014" format so that twitter.Status behaves correctly
        # database returns it as a datetime
        created_at_str = tweet_dict['created_at'].strftime(constants.TWITTER.TWITTER_API_TIME_FORMAT)
        tweet_dict['created_at'] = created_at_str

        # split off twitter user from db dict (all fields start with 'user_' - we will strip that)
        user_dict = {k[len("user_"):]: v for k, v in db_dict.iteritems() if k.startswith("user_")}

        # fix created_at value
        created_at_str = user_dict['created_at'].strftime(constants.TWITTER.TWITTER_API_TIME_FORMAT)
        user_dict['created_at'] = created_at_str

        # set it so the twitter.Status object will be initialized correctly
        tweet_dict['user'] = user_dict

        # wrap the tweet
        tweet_inst = cls(tweet_dict, unanswered=tweet_dict['unanswered'])

        return tweet_inst

    def __str__(self):
        sb = []
        for k in self.__dict__:
            if k == 'tweet_inst':
                value = simplejson.dumps(self.tweet_inst.AsDict(), sort_keys=True, indent=2, ensure_ascii=False)
            else:
                value = self.__dict__[k]
            sb.append("{}='{}'".format(k, value))
        return ", ".join(sb)

    def __repr__(self):
        return "%s: %r" % (self.__class__, self.__str__())



class TLTwitterUser(twitter.User):

    db = db.TLInsightsDB(constants.DB.DB_TEST_NAME)

    def __init__(self):
        pass

    @classmethod
    def get_user_by_id(cls, id):
        return id
