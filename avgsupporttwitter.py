#! /usr/bin/env python

__author__ = 'cappy'

import datetime
from dateutil.parser import parse
import excelwriter
from re import search, IGNORECASE
from pprint import pprint
import os.path
import argparse
from errno import EEXIST
import json
import twitter
import csv
from csvdict import DictUnicodeWriter
from tlinsights.constants import DB, TWITTER
from tlinsights import db


def make_sure_path_exists(path):
    """ checks to see if path exists and creates it if it does not.

    Handles race condition present if the path is created between the os.pathexists() and os.makedirs() calls
    """
    try:
        os.makedirs(path)
    except OSError as exception:
        if EEXIST != exception.errno:  # ignore the error if the path already exists
            raise


def get_url(self):
    # http://twitter.com/{twitter-user-id}/status/{tweet-status-id}
    return "http://twitter.com/%s/status/%s" % (str(self.user.id), str(self._id))


def pretty_tweet(self, allow_non_ascii=False):
    '''A JSON string representation of this twitter.Status instance.

    To output non-ascii, set keyword allow_non_ascii=True.

    :param self:
    :param allow_non_ascii:
    Returns:
      A JSON string representation of this twitter.Status instance
   '''
    return twitter.simplejson.dumps(self.AsDict(), sort_keys=True, indent=2,
                                    ensure_ascii=not allow_non_ascii)


def GetTweetDetail(self):
    s = "%s [%s]" % (self.text, self.GetTweetTimeForExcel())
    return s


def GetTweetTimeForExcel(self):
    s = parse(self.created_at).strftime(TWITTER.TWITTER_TIME_FORMAT)
    return s

# hacky way to extend a class at runtime but don't know how to do it otherwise
twitter.Status.GetUrl = get_url
twitter.Status.AsJsonString = pretty_tweet
twitter.Status.GetTweetDetail = GetTweetDetail
twitter.Status.GetTweetTimeForExcel = GetTweetTimeForExcel

# used because python has no labeled breaks - we will throw this when Twitter throws an
# exception due to hitting rate limit or over capacity - two things we can do nothing about
class TwitterUnrecoverableException(Exception):
    pass

# column headers for our output csv file and for
# csv_row dictionary

csv_headers = ["PostId",
               "Post",
               "PostDate",
               "PostMessage",
               "ReplyDate",
               "ReplyMessage",
               "GOS"]

# will hold Twitter API instance - lazy init below
api = None

twitter_data_dir = "twitter-gos-data"
tweets_file = "%s/tweets.json" % twitter_data_dir
dead_tweets_file = "{}/dead_tweets.json".format(twitter_data_dir)


def excel_date(datestring):
    temp = datetime.datetime(1899, 12, 30)
    date1 = datetime.datetime.strptime(datestring, "%m/%d/%Y %I:%M:%S %p")
    delta = date1 - temp
    return float(delta.days) + (float(delta.seconds) / 86400)


def load_tweets_from_csv(filename):
    # TODO: make these sets - we just iterate over them
    tweets = {'avg': [], 'user': []}
    csv_file = "{}/{}".format(twitter_data_dir, filename)
    if os.path.isfile(csv_file):
        with open(csv_file, 'rb') as fp:
            reader = csv.DictReader(fp)
            for index, row in enumerate(reader):
                # skip the header row and any retweets since we don't care about them,
                # also filter by author == AVGFree
                if index != 0 and 'RT @' not in row['CONTENT']:
                    tweet_id = int(row['ARTICLE_URL'].rsplit("/", 1)[1])
                    if ("AVGFREE" == row['AUTHOR'] or "AVGSUPPORT" == row['AUTHOR']):
                        tweets['avg'].append(tweet_id)
                    else:
                        tweets['user'].append(tweet_id)


    else:
        print "%s - FILE NOT FOUND" % csv_file

    pprint(tweets)
    return tweets


def load_tweets_from_json():
    """
    loads the cached tweets we've saved from previous runs - to get around rate limiter
    TODO - should put this in a simplified data structure or a SQLLite db
    :return:
    a dict containing all the tweets we've seen before
    """
    rehydrated_tweets = {}  # will hold full Status objects
    tweets_to_cache = {}  # will hold tweets that are to be cached at end of run
    dead_tweets = []
    if os.path.isfile(tweets_file):
        try:
            with open(tweets_file, 'rb') as fp:
                # Python Twitter API can't store full Status (tweet) objects in JSON for whatever reason
                # so we store them as raw dicts and load those via JSON decoder
                tweet_dicts = json.load(fp)

                for json_raw_tweet in tweet_dicts:
                    # this reconstitutes each of the barebones JSON dicts into full Status objects
                    tw = twitter.Status.NewFromJsonDict(json_raw_tweet)
                    id = tw.GetId()
                    rehydrated_tweets[id] = tw
                    tweets_to_cache[id] = json_raw_tweet

        except ValueError as ve:
            pprint(ve)
    else:
        print "%s - FILE NOT FOUND" % tweets_file

    if os.path.isfile(dead_tweets_file):
        try:
            with open(dead_tweets_file, 'rb') as fp:
                # Python Twitter API can't store full Status (tweet) objects in JSON for whatever reason
                # so we store them as raw dicts and load those via JSON decoder
                dead_tweets = json.load(fp)

        except ValueError as ve:
            pprint(ve)
    else:
        print "%s - FILE NOT FOUND" % dead_tweets_file

    print("{} tweet loaded from cache".format(len(rehydrated_tweets)))

    return rehydrated_tweets, tweets_to_cache, dead_tweets


def add_csv_row(already_seen, csv_data, excel_data, saved_tweets):
    # verify that we've found first touch and user tweets and placed them in our data structure
    if saved_tweets["first_touch"] and saved_tweets["user_tweet"]:

        # PostID	Post	PostDate	PostMessage	ReplyDate	ReplyMessage	GOS

        csv_first_row = {}

        # shortcuts to make following code block easier to follow
        ft = saved_tweets["first_touch"]
        fs = saved_tweets["first_support"]
        ut = saved_tweets["user_tweet"]

        # fill out the csv data row
        csv_first_row["PostId"] = ut.GetId()
        csv_first_row["Post"] = ut.GetUrl()
        csv_first_row["PostDate"] = ut.GetTweetTimeForExcel()
        csv_first_row["PostMessage"] = ut.GetText()  # .encode('utf8')

        csv_first_row["ReplyDate"] = ft.GetTweetTimeForExcel()
        csv_first_row["ReplyMessage"] = ft.GetText()  # .encode('utf8')
        csv_first_row["GOS"] = time_between_tweets_in_hours(ut, ft)

        # pprint(csv_first_row)

        csv_data["first"].append(csv_first_row)
        db_list = csv_first_row.copy()  # shallow-copy the dict to add new fields to not upset old code
        db_list["ReplyPostId"] = ft.GetId()  # needed for DB
        db_list["GOSType"] = "first_touch"
        db.save_gos_interaction(db_list)

        excel_first_row = [
            csv_first_row["PostId"],
            csv_first_row["Post"],
            csv_first_row["PostDate"],
            csv_first_row["PostMessage"],
            csv_first_row["ReplyDate"],
            csv_first_row["ReplyMessage"],
            csv_first_row["GOS"]
        ]

        excel_data["TW-First Touch Data"].append(excel_first_row)

        # only fill in support data if it's present
        if fs:
            csv_support_row = {}

            csv_support_row["PostId"] = ut.GetId()
            csv_support_row["Post"] = ut.GetUrl()
            csv_support_row["PostDate"] = ut.GetTweetTimeForExcel()
            csv_support_row["PostMessage"] = ut.GetText()  # .encode('utf-8')

            csv_support_row["ReplyDate"] = fs.GetTweetTimeForExcel()
            csv_support_row["ReplyMessage"] = fs.GetText()  # .encode('utf-8')
            csv_support_row["GOS"] = time_between_tweets_in_hours(ut, fs)

            # pprint(csv_support_row)

            csv_data["support"].append(csv_support_row)
            # TODO: fix this
            db_list = csv_support_row.copy()  # shallow-copy the dict for DB to not mess up existing code
            db_list["ReplyPostId"] = fs.GetId()  # needed for DB
            db_list["GOSType"] = "support"
            db.save_gos_interaction(db_list)

            excel_support_row = [
                csv_support_row["PostId"],
                csv_support_row["Post"],
                csv_support_row["PostDate"],
                csv_support_row["PostMessage"],
                csv_support_row["ReplyDate"],
                csv_support_row["ReplyMessage"],
                csv_support_row["GOS"]
            ]

            excel_data["TW-Support First Touch Data"].append(excel_support_row)

    elif already_seen:
        print "<<ALREADY PROCESSED THREAD>>"
        # already_seen = False
    else:
        # was a root tweet (like a status update that no one cared about)
        print "NO REPLIES"
    print "-----"


def process_tweet(last_tweet_was_user_tweet, saved_tweets, tw, cached, tweets_from_csv):
    """
    categorizes the passed-in tweet as a user tweet, first response tweet, or support tweet
    :param last_tweet_was_user_tweet: flag to indicate if last tweet processed was a user tweet (to detect dupes)
    :param saved_tweets: data structure containing the last user, first touch, and support tweets we've found
    :param tw: the raw tweet from the API or cache
    :return: last_tweet_was_user_tweet [True|False]
    """

    # debugging aid
    c = 'C' if cached else 'N'

    # shortcuts for below blocks
    text = tw.GetTweetDetail()
    url = tw.GetUrl()
    # is this a user tweet?
    if tw.user.id not in TWITTER.TWITTER_USER_IDS:
        # and not multiple user tweets in a row?
        if not last_tweet_was_user_tweet:
            saved_tweets["user_tweet"] = tw  # save it in our data structure to track important ones
            print u"[USER: {}][{}] {} [{}]".format(tw.user.name, c, text, url)
            last_tweet_was_user_tweet = True
        else:
            # it's a user tweet AFTER a user tweet, so skip it and we'll keep the first one
            print u"[MULTIUSER- IGNORE][{}] {} [{}]".format(c, text, url)

        # remove it from the dict of user tweets to track tweets that AVG has not responded to
        if tw.id in tweets_from_csv['user']:
            tweets_from_csv['user'].remove(tw.id)
            print "Removing user tweet {}".format(tw.id)
    else:  # one of our tweets
        last_tweet_was_user_tweet = False
        if search(r"\^JM|\^ZP|\^ZCS|\^AHS|#AVGSupport|@AVGSupport", tw.text,
                  IGNORECASE):  # scan tweet text for support indicators
            saved_tweets["first_support"] = tw
            saved_tweets["first_touch"] = tw  # support tweets are also first touch tweets
            print u"[SUPPORT/FIRST: {}][{}] {} [{}]".format(tw.user.name, c, text, url)
        elif tw.in_reply_to_status_id:
            # we don't care about threads when we're at the root. This won't be a
            # problem when running for real but will be in testing. We only care about
            # tweets we make that are replies. This will make sure we don't falsely set
            # our root tweet as our first response
            saved_tweets["first_touch"] = tw
            print u"[FIRST: {}][{}] {} [{}]".format(tw.user.name, c, text, url)
        else:
            print u"[SOLO ROOT TWEET: {}][{}] {} [{}]".format(tw.user.name, c, text, url)

    return last_tweet_was_user_tweet


def write_cached_tweets(cached_tweets, tweets_not_found):
    with open(tweets_file, "wb ") as fp:
        json.dump(cached_tweets.values(), fp)
    with open(dead_tweets_file, "wb") as fp:
        json.dump(tweets_not_found, fp)


def cache_tweet(cached_tweets, tw):
    """
    cache the passed in tweet if not already slated for caching
    :param cached_tweets: dict we'll use to write out to disk
    :param tw: raw tweet - NON JSON-SERIALIZABLE
    :return:
    """
    if tw.GetId() not in cached_tweets.keys():
        # BUG BUG BUG
        # we don't use the retweet value so this is fine, but I don't like doing things like this
        # this is hack to get JSON unpacking to work
        tw.SetCurrent_user_retweet(None)
        s = tw.AsDict()
        cached_tweets[tw.GetId()] = s

    return cached_tweets


def write_unanswered_tweets(user_tweet_ids, tweets_to_cache, tweets_not_found, excel_data):
    try:

        for tweet_id in user_tweet_ids:
            # only hit the API if we need to
            if tweet_id in tweets_not_found:
                print "[TWEET NOT FOUND {} - SKIPPING]".format(tweet_id)
                continue
            if tweet_id not in tweets_to_cache.keys():
                try:
                    global api
                    if not api:
                        api = twitter.Api(consumer_key=TWITTER.TWITTER_CONSUMER_KEY,
                                          consumer_secret=TWITTER.TWITTER_CONSUMER_SECRET,
                                          access_token_key=TWITTER.TWITTER_ACCESS_KEY,
                                          access_token_secret=TWITTER.TWITTER_ACCESS_SECRET)

                    tweet = api.GetStatus(tweet_id, include_entities=False)

                    # store the tweet in the cache so we won't have to look it up again
                    tweets_to_cache = cache_tweet(tweets_to_cache, tweet)

                    tw = tweet.AsDict()
                except twitter.TwitterError as te:
                    code = te.args[0][0]["code"]
                    pprint(te)
                    # you'll occasionally get 404's etc as users delete tweets
                    if 88 == code or 130 == code:
                        print "%s... TRY AGAIN IN 15 MINUTES" % te.args[0][0]["message"]
                        rls = api.GetRateLimitStatus()
                        print "RATE LIMIT WILL RENEW AT {}".format(
                            datetime.datetime.fromtimestamp(
                                rls["resources"]["statuses"]["/statuses/lookup"]["reset"]
                            ).strftime("%I:%M:%S %p")
                        )
                        raise TwitterUnrecoverableException
                    if 34 == code:
                        # tweet not found - either the original user deleted it or twitter is not
                        # providing it via the API.
                        print "TWEET {} NOT FOUND - REMOVING FROM FUTURE RUNS".format(tweet_id)
                        tweets_not_found.append(tweet_id)  # save the tweet in our cache of '34' tweets
                    continue
            else:
                # can this ever happen? Can we have a tweet in the cache that is unreplied to? I don't think so
                print "This tweet was in cache but unanswered: {}".format(tweet_id)
                tw = tweets_to_cache[tweet_id]

            row = []
            row.append(tw["id"])
            row.append("http://twitter.com/{}/status/{}".format(tw["user"]["id"], tw['id']))
            row.append(parse(tw["created_at"]).strftime(TWITTER.TWITTER_TIME_FORMAT))
            row.append(tw["text"])

            pprint(row)

            excel_data['TW-Unanswered'].append(row)

            # twitter threw exception we can't recover from, write out what we can
    except TwitterUnrecoverableException:
        write_cached_tweets(tweets_to_cache)
        return


def write_output_csv(csv_data, csv_file, csv_headers):
    if len(csv_data):
        # write our data to the CSV file
        with open(csv_file, 'wb') as fou:
            fou.write(u'\ufeff'.encode('utf8'))  # need this line to make Excel treat CSV as Unicode
            dw = DictUnicodeWriter(fou, fieldnames=csv_headers)
            dw.writeheader()
            for result_row in csv_data:
                dw.writerow(result_row)


def write_to_excel(excel_data, filename="TW-FirstTouchDataTest.xlsx"):
    if len(excel_data['TW-First Touch Data']):
        w = excelwriter.ExcelWriter()

        w.create_workbook("{}/{}".format(twitter_data_dir, filename))

        for sheet_name, twitter_data in excel_data.iteritems():
            with open("{}/{}.json".format(twitter_data_dir, sheet_name), "wb") as fp:
                json.dump(twitter_data, fp)
            w.add_sheet(sheet_name, twitter_data)

        w.close_workbook()


def get_twitter_gos(cmd_line_args):
    """
    Opens a list of Tweets exported from Radian6 and calculates the Gauge of Service
    for them. I use Radian6 because it's the only way I can be guaranteed to get all our tweets between 2 dates easily.
    Twitter only serves up your last 3200 tweets and search is VERY limited to only popular and recent tweets - up to
    Twitter's discretion, effectively making it useless for us in this case.

    :param cmd_line_args: tweet id if passed

    For each tweet we find from Radian6, I:

    - get the ID from the URL
    - use ID to look up tweet from Twitter API (only time we hit API) - pausing 0.2 sec after each call [pause is off]
    - check if tweet is by AVG or not
    - walk the conversation back from original tweet to last user tweet
    - keep track of which tweets are from support (have a '^JM' or '#AVGSupport' hashtag for example)
    - after processing whole conversation we've got original user tweet and our first replies (support + regular) - note
        that our first reply may be BOTH a support and a regular tweet (quite often actually)
    - calculate time difference in hours between them
    - write out an Excel-friendly CSV containing the data (see csv_headers variable for what's contained in file)
    - write out a table to an Excel file containing the data (done after the CSV code)

    Notes:

    - VERY hard to correlate this data with the Twitter website; Twitter removes tweets as it sees fit
    - from https://support.twitter.com/articles/277671-i-m-missing-tweets:

    "@replies do sometimes appear when the original Tweet is expanded, but due to capacity restrictions
    not all @replies will appear in an expanded Tweet."

    "Tweets more than a week old may fail to display in timelines or search because of indexing capacity
    restrictions. Old Tweets are never lost, but cannot always be displayed."

    :return:
    nothing

    """

    # sets allow constant-time lookup - perfect for membership tests
    # We'll use this to hold tweets we've already seen while we walk back
    # through conversations - since the list of tweets we get from Radian6 is all of our
    # tweets between 2 dates we will get multiple from an individual conversation. We only
    # process a conversation thread once; therefore, as we call the API we'll get tweets
    # back from it that are ALSO in the list of tweets from Radian6. We'll skip these.
    processed = set()

    # used to hold the data we'll write to the CSV at the end; we'll create 2 csv's: one for first touch
    # and one for support
    csv_data = {"first": [],
                "support": []}

    # used to hold data for Excel
    excel_data = {"TW-First Touch Data": [],
                  "TW-Support First Touch Data": [],
                  "TW-Unanswered": []}

    # get the list of tweets we get from Radian6's XML or CSV export
    # for testing you can use a slice of the list (every nth element where n is
    # the number after the ::)
    # tweet_ids_from_csv = load_tweets_from_csv()#[::22]
    # for individual  unit testing
    tweet_ids_from_csv = {'avg': [cmd_line_args.status_id]} if cmd_line_args.status_id else load_tweets_from_csv(
        cmd_line_args.input)
    # tweet_ids_from_csv = ["525654285120708608"]

    # filename of our output file
    csv_first_file = "%s/first_touch_results.csv" % twitter_data_dir
    csv_support_file = "%s/first_support_results.csv" % twitter_data_dir

    # flag used to track multiple user tweets in a row...
    # we only want the first tweet by a user to us if they tweet
    # multiple times as they can send follow-up "WHERE THE F ARE YOU" messages.
    # They can also send more tweets with more info. We want their first tweet
    # in a series. This is a judgement call.
    last_tweet_was_user_tweet = False

    # the Twitter Python API is whack and won't let us JSON-encode tweets (Status objects.) We
    # have to 'reconstitute' them from dicts that the twitter.Status.AsDict() method provides.
    # This means we can't simply use one data structure with all the data. The 'cached_tweets' dict
    # holds FULLY-RECONSTITUTED STATUS OBJECTS we get from the offline cache. tweets_to_cache will
    # hold just-dict representations of cached tweets + new tweets found during this run
    # that need to be cached. They will be stored as dicts during the run, then JSON-serialized and written
    # to the cache (overwritten each time - can't figure out how to append) on exit. On next run they will
    # be reconstituted into full Status objects.
    # tweets_not_found: tweets that return 'not found' error 34 from the API - we are tracking them so that we
    # don't call the API for them using up one of our valuable 180 calls per 15 minutes via the rate limiter.
    # Note we can't use a set here because they are not serializable to JSON by default
    cached_tweets, tweets_to_cache, tweets_not_found = load_tweets_from_json()

    try:
        for tweet in tweet_ids_from_csv['avg']:

            # data structure to hold the latest tweets in our
            # parsing of a conversation thread
            support_tweets = {"user_tweet": None,
                              "first_touch": None,
                              "first_support": None}

            tweet_id = tweet  # cache id for inner loop

            # used as a flag for debugging output
            already_seen = False  # this is a tweet we've never seen before as far as we know

            # ID of the tweet reply
            parent_id = None

            # while we have a valid tweet (ID) from an AVG account
            while tweet_id:

                # only hit the API for tweets we've not seen before... As we walk back through
                # conversations we will come across cases where one of our tweets in the original list
                # from Radian6 import is also a tweet we get back from the API
                if tweet_id not in processed:

                    processed.add(tweet_id)  # store the tweet id in the set so we don't process it again

                    # for debugging output
                    cached = False

                    # check to see if we have a fully-reconstituted Status object from the cache. If so,
                    # use it instead of hitting the API
                    # TODO: This should be a DB call
                    if tweet_id in cached_tweets.keys():
                        tw = cached_tweets[tweet_id]
                        # again, debugging flag only
                        cached = True
                    elif tweet_id in tweets_not_found:
                        # if this tweet is one of the ones that twitter returns a status 34 for (not found) then just
                        # skip it - not worth wasting a valuable rate-limited API call on it again
                        print "[DEAD TWEET DETECTED {} - REMOVING THREAD]".format(tweet_id)
                        tweet_id = None # to break the loop on the next iteration
                        continue
                    else:
                        try:
                            global api  # Python scope rules: any global assigned locally is overwritten and becomes local
                            if not api:
                                api = twitter.Api(consumer_key=TWITTER.TWITTER_CONSUMER_KEY,
                                                  consumer_secret=TWITTER.TWITTER_CONSUMER_SECRET,
                                                  access_token_key=TWITTER.TWITTER_ACCESS_KEY,
                                                  access_token_secret=TWITTER.TWITTER_ACCESS_SECRET)

                            # get the next tweet via Twitter API,
                            # remove all entities (pics, vids, etc.)
                            tw = api.GetStatus(tweet_id, include_entities=False)

                            # store the tweet in the cache so we won't have to look it up again
                            tweets_to_cache = cache_tweet(tweets_to_cache, tw)

                            # sleep(0.2) # play nice with the API to avoid rate limits

                        except twitter.TwitterError as te:
                            code = te.args[0][0]["code"]
                            pprint(te)
                            # you'll occasionally get 404's etc as users delete tweets
                            if 88 == code or 130 == code:
                                print "%s... TRY AGAIN IN 15 MINUTES" % te.args[0][0]["message"]
                                rls = api.GetRateLimitStatus()
                                print "RATE LIMIT WILL RENEW AT {}".format(
                                    datetime.datetime.fromtimestamp(
                                        rls["resources"]["statuses"]["/statuses/lookup"]["reset"]
                                    ).strftime("%I:%M:%S %p")
                                )
                                raise TwitterUnrecoverableException  # break out of inner loop
                            if 34 == code:
                                # tweet not found - either the original user deleted it or twitter is not
                                # providing it via the API.
                                tweets_not_found.append(tweet_id)  # save the tweet in our cache of '34' tweets

                                if parent_id:
                                    # we are in the middle of conversation thread - clear the support tweets
                                    # and move on with the next conversation. Also cache the tweet ID so we can track
                                    # these and ignore in the future
                                    support_tweets = None
                                    print("[DEAD THREAD - REMOVING]")

                                    # TODO: where do we go from here?

                            continue  # resume next loop iteration and try again

                    last_tweet_was_user_tweet = process_tweet(last_tweet_was_user_tweet,
                                                              support_tweets,
                                                              tw,
                                                              cached,
                                                              tweet_ids_from_csv)
                else:
                    # set flag for debugging output below, can happen when we break out from one of the loops above
                    already_seen = True
                    print "[ALREADY SEEN THIS ID] %s" % str(tweet_id)
                    break

                # follow the conversation thread back by setting the id of the
                # next tweet to the previous one in the thread
                tweet_id = tw.in_reply_to_status_id

                # store the current tweet id so we can check if we're in the middle of processing a thread when
                # we get a '34' (page not found) error from the API. If so, we'll chuck the whole thread so we don't
                # end up with negative GoS values
                parent_id = tw.id

            # add a row to the data structure we'll write to a CSV file; however, a '34' error ('not found') could
            # end the thread - have to check if we have any support tweets first
            if support_tweets:
                add_csv_row(already_seen, csv_data, excel_data, support_tweets)

            # clean up
            del support_tweets

    # twitter threw exception we can't recover from, write out what we can
    except TwitterUnrecoverableException:
        write_cached_tweets(tweets_to_cache)
        return

    # there may not be user tweets if we have a single tweet ID as a command line argument...
    if tweet_ids_from_csv.has_key('user'):
        write_unanswered_tweets(tweet_ids_from_csv['user'], tweets_to_cache, tweets_not_found, excel_data)

    # write out tweets to cache
    write_cached_tweets(tweets_to_cache, tweets_not_found)

    write_output_csv(csv_data["first"], csv_first_file, csv_headers)
    write_output_csv(csv_data["support"], csv_support_file, csv_headers)

    write_to_excel(excel_data, cmd_line_args.output)


def time_between_tweets_in_hours(tw1, tw2):
    tweet1_time = parse(tw1.created_at)
    tweet2_time = parse(tw2.created_at)
    diff = tweet2_time - tweet1_time
    return "{:0.1f}".format(diff.total_seconds() / 60 / 60)


if __name__ == '__main__':
    # -i 1_1416582603663159.csv -o TwitterData_11_21.xlsx
    parser = argparse.ArgumentParser(description="Creates csv containing Gauge of Service data for tweets",
                                     version='%(prog)s 1.0')
    parser.add_argument("-id", "--status_id", type=int, help="Tweet ID")
    # parser.add_argument("-t", "--access_token", type=str, default="rDFfVkx9dIyfjdni3AUEnA", nargs="?",
    # help='Twitter access token')
    parser.add_argument("-o", "--output", type=str, help="output file [*.xlsx]")
    parser.add_argument("-i", "--input", type=str, help="input file [*.csv]")
    args = parser.parse_args()
    get_twitter_gos(args)

