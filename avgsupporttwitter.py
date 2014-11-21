#! /usr/bin/env python
__author__ = 'cappy'

import datetime
import time
from dateutil.parser import parse
import excelwriter
from re import search, IGNORECASE
from pprint import pprint
import os.path
import argparse
import errno
import json
import twitter
import csv
from csvdict import DictUnicodeWriter, DictUnicodeReader


def make_sure_path_exists(path):
    """ checks to see if path exists and creates it if it does not.

    Handles race condition present if the path is created between the os.pathexists() and os.makedirs() calls
    """
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:  # ignore the error if the path already exists
            raise

def GetUrl(self):
    #http://twitter.com/{twitter-user-id}/status/{tweet-status-id}
    return "http://twitter.com/%s/status/%s" % (str(self.user.id), str(self._id))

def PrettyTweet(self, allow_non_ascii=False):

    '''A JSON string representation of this twitter.Status instance.

    To output non-ascii, set keyword allow_non_ascii=True.

    Returns:
      A JSON string representation of this twitter.Status instance
   '''
    return twitter.simplejson.dumps(self.AsDict(), sort_keys=True, indent=2,
               ensure_ascii= not allow_non_ascii )

def GetTweetDetail(self):
    s = "%s [%s]" % (self.text, self.GetTweetTimeForExcel())
    return s

def GetTweetTimeForExcel(self):
    s = parse(self.created_at).strftime("%m/%d/%Y %I:%M:%S %p")
    return s

# hacky way to extend a class at runtime but don't know how to do it otherwise
twitter.Status.GetUrl = GetUrl
twitter.Status.AsJsonString = PrettyTweet
twitter.Status.GetTweetDetail = GetTweetDetail
twitter.Status.GetTweetTimeForExcel = GetTweetTimeForExcel

TWITTER_CONSUMER_KEY = 'riHaA3FNKboupg5jsW1S1gkCl'
TWITTER_CONSUMER_SECRET = 'NnJ6NdQFY5r1RRwOrBwDiZXWurkfApDG15kvT0ZDiN0ClaSISi'
TWITTER_ACCESS_KEY = '61781392-S52Cc52hjreuzQZpKB4SevYMHlLQo3OWLLOBuDB0c'
TWITTER_ACCESS_SECRET = 'TQSD6PdGryWg9RrXubJTCaVT3MgOWK2BRJws6xyXZvzYp'

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

def excel_date(datestring):
    temp = datetime.datetime(1899, 12, 30)
    date1 = datetime.datetime.strptime(datestring, "%m/%d/%Y %I:%M:%S %p")
    delta = date1 - temp
    return float(delta.days) + (float(delta.seconds) / 86400)

def load_tweets_from_csv(filename):
    tweets = {'avg':[], 'user':[]}
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
    rehydrated_tweets = {} # will hold full Status objects
    tweets_to_cache = {} # will hold tweets that are to be cached at end of run
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

    print("{} tweet loaded from cache".format(len(rehydrated_tweets)))
    #pprint(tweets_to_cache)

    return (rehydrated_tweets,tweets_to_cache)


def add_csv_row(already_seen, csv_data, excel_data, saved_tweets):
    # verify that we've found first touch and user tweets and placed them in our data structure
    if saved_tweets["first_touch"] and saved_tweets["user_tweet"]:

#PostID	Post	PostDate	PostMessage	ReplyDate	ReplyMessage	GOS

        csv_first_row = {}

        # shortcuts to make following code block easier to follow
        ft = saved_tweets["first_touch"]
        fs = saved_tweets["first_support"]
        ut = saved_tweets["user_tweet"]

        # fill out the csv data row
        csv_first_row["PostId"] = ut.GetId()
        csv_first_row["Post"] = ut.GetUrl()
        csv_first_row["PostDate"] = ut.GetTweetTimeForExcel()
        csv_first_row["PostMessage"] = ut.GetText()#.encode('utf8')

        csv_first_row["ReplyDate"] = ft.GetTweetTimeForExcel()
        csv_first_row["ReplyMessage"] = ft.GetText()#.encode('utf8')
        csv_first_row["GOS"] = time_between_tweets_in_hours(ut, ft)

        #pprint(csv_first_row)

        csv_data["first"].append(csv_first_row)

        excel_first_row = [
            csv_first_row["PostId"],
            csv_first_row["Post"],
            csv_first_row["PostDate"],
            csv_first_row["PostMessage"],
            csv_first_row["ReplyDate"],
            csv_first_row["ReplyMessage"],
            csv_first_row["GOS"]
        ]

        excel_data["first"].append(excel_first_row)

        # only fill in support data if it's present
        if fs:

            csv_support_row = {}

            csv_support_row["PostId"] = ut.GetId()
            csv_support_row["Post"] = ut.GetUrl()
            csv_support_row["PostDate"] = ut.GetTweetTimeForExcel()
            csv_support_row["PostMessage"] = ut.GetText()#.encode('utf-8')

            csv_support_row["ReplyDate"] = fs.GetTweetTimeForExcel()
            csv_support_row["ReplyMessage"] = fs.GetText()#.encode('utf-8')
            csv_support_row["GOS"] = time_between_tweets_in_hours(ut, fs)

            #pprint(csv_support_row)

            csv_data["support"].append(csv_support_row)

            excel_support_row = [
                csv_support_row["PostId"],
                csv_support_row["Post"],
                csv_support_row["PostDate"],
                #csv_support_row["PostDateReal"],
                csv_support_row["PostMessage"],
                csv_support_row["ReplyDate"],
                csv_support_row["ReplyMessage"],
                csv_support_row["GOS"]
            ]

            excel_data["support"].append(excel_support_row)

    elif already_seen:
        print "<<ALREADY PROCESSED THREAD>>"
        already_seen = False
    else:
        # was a root tweet (like a status update that no one cared about)
        print "NO REPLIES"
    print "-----"


def process_tweet(last_tweet_was_user_tweet, saved_tweets, tw, cached, tweets_to_use):
    """
    categorizes the passed-in tweet as a user tweet, first response tweet, or support tweet
    :param last_tweet_was_user_tweet: flag to indicate if last tweet processed was a user tweet (to detect dupes)
    :param saved_tweets: data structure containing the last user, first touch, and support tweets we've found
    :param tw: the raw tweet from the API or cache
    :return: last_tweet_was_user_tweet [True|False]
    """
    # IDs of AVG Twitter accounts - used to flag tweets
    # as coming from us or users
    avg_twitter_ids = [61781392, #AVGFree
                       142353967] #AVGSupport

    # debugging aid
    c = 'C' if cached else 'N'

    # shortcuts for below blocks
    text = tw.GetTweetDetail()
    url = tw.GetUrl()
    # is this a user tweet?
    if tw.user.id not in avg_twitter_ids:
        # and not multiple user tweets in a row?
        if not last_tweet_was_user_tweet:
            saved_tweets["user_tweet"] = tw  # save it in our data structure to track important ones
            print u"[USER: {}][{}] {} [{}]".format(tw.user.name, c, text, url)
            last_tweet_was_user_tweet = True
        else:
            # it's a user tweet AFTER a user tweet, so skip it and we'll keep the first one
            print u"[MULTIUSER- IGNORE][{}] {} [{}]".format(c, text, url)

        #remove it from the dict of user tweets to track tweets with missing responses
        if tw.id in tweets_to_use['user']: tweets_to_use['user'].remove(tw.id)
        print "Removing user tweet {}".format(tw.id)
    else:  # one of our tweets
        last_tweet_was_user_tweet = False
        if search(r"\^JM|\^ZP|#AVGSupport|@AVGSupport", tw.text, IGNORECASE):  # scan tweet text for support indicators
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

def write_cached_tweets(cached_tweets):
    with open(tweets_file, "wb ") as fp:
        json.dump(cached_tweets.values(), fp)


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

def write_output_csv(csv_data, csv_file, csv_headers):
    # write our data to the CSV file
    with open(csv_file, 'wb') as fou:
        fou.write(u'\ufeff'.encode('utf8'))  # need this line to make Excel treat CSV as Unicode
        dw = DictUnicodeWriter(fou, fieldnames=csv_headers)
        dw.writeheader()
        for result_row in csv_data:
            dw.writerow(result_row)

def write_to_excel(excel_data, filename="TW-FirstTouchDataTest.xlsx"):

    sheet_names = [{"name": "TW-First Touch Data", "key": "first"},
                   {"name": "TW-Support First Touch Data", "key": "support"}]

    w = excelwriter.ExcelWriter()

    w.create_workbook("{}/{}".format(twitter_data_dir, filename))

    for sheet in sheet_names:
        w.add_sheet(sheet["name"])
        w.write_twitter_gos_data_for(sheet["name"], csv_headers, excel_data[sheet["key"]])

    w.close_workbook()

def get_twitter_gos(args):
    """
    Opens a list of Tweets exported from Radian6 and calculates the Gauge of Service
    for them. I use Radian6 because it's the only way I can be guaranteed to get all our tweets between 2 dates easily.
    Twitter only serves up your last 3200 tweets and search is VERY limited to only popular and recent tweets - up to
    Twitter's discretion, effectively making it useless for us in this case.

    For each tweet we find from Radian6, I:

    - get the ID from the URL
    - use ID to look up tweet from Twitter API (only time we hit API) - pausing 0.3 sec after each call
    - check if tweet is by AVG or not
    - walk the conversation back from original tweet to last user tweet
    - keep track of which tweets are from support (have a '^JM' or '#AVGSupport' hashtag)
    - after processing whole conversation we've got original user tweet and our first replies (support + regular) - note
        that our first reply may be BOTH a support and a regular tweet (quite often actually)
    - calculate time difference in hours between them
    - write out an Excel-friendly CSV containing the data (see csv_headers variable for what's contained in file)

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
    csv_data = {"first":[],
                "support":[]}

    # used to hold data for Excel
    excel_data = {"first": [],
                  "support": []}

    # get the list of tweets we get from Radian6's XML or CSV export
    # for testing you can use a slice of the list (every nth element where n is
    # the number after the ::)
    #tweets_to_use = load_tweets_from_csv()#[::22]
    # for individual  unit testing
    tweets_to_use = [args.status_id] if args.status_id else load_tweets_from_csv(args.input)
    #tweets_to_use = ["525654285120708608"]

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
    cached_tweets, tweets_to_cache = load_tweets_from_json()

    # used because python has no labeled breaks - we will throw this when Twitter throws an
    # exception due to hitting rate limit or over capacity - two things we can do nothing about
    class TwitterUnrecoverableException(Exception):
        pass

    try:
        for tweet in tweets_to_use['avg']:

            # data structure to hold the latest tweets in our
            # parsing of a conversation thread
            saved_tweets = {"user_tweet": None,
                            "first_touch": None,
                            "first_support": None}

            tweet_id = tweet # cache id for inner loop

            # used as a flag for debugging output
            already_seen = False # this is a tweet we've never seen before as far as we know

            # while we have a valid tweet (ID)
            while tweet_id:

                # only hit the API for tweets we've not seen before... As we walk back through
                # conversations we will come across cases where one of our tweets in the original list
                # from Radian6 import is also a tweet we get back from the API
                if tweet_id not in processed:

                    processed.add(tweet_id) # store the tweet id so we don't process it again

                    # for debugging output
                    cached = False

                    if tweet_id in cached_tweets.keys():
                        tw = cached_tweets[tweet_id]
                        cached = True
                    else:
                        try:
                            global api
                            if not api:
                                api = twitter.Api(consumer_key=TWITTER_CONSUMER_KEY,
                                                  consumer_secret=TWITTER_CONSUMER_SECRET,
                                                  access_token_key=TWITTER_ACCESS_KEY,
                                                  access_token_secret=TWITTER_ACCESS_SECRET)

                            # get the next tweet via Twitter API,
                            # remove all entities (pics, vids, etc.)
                            tw = api.GetStatus(tweet_id, include_entities=False)

                            # store the tweet in the cache so we won't have to look it up again
                            tweets_to_cache = cache_tweet(tweets_to_cache, tw)

                            #sleep(0.2) # play nice with the API to avoid rate limits

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
                                raise TwitterUnrecoverableException # break out of inner loop
                            continue # resume next loop iteration and try again

                    last_tweet_was_user_tweet = process_tweet(last_tweet_was_user_tweet,
                                                              saved_tweets,
                                                              tw,
                                                              cached,
                                                              tweets_to_use)
                else:
                    # set flag for debugging output below
                    already_seen = True
                    print "[ALREADY SEEN THIS ID] %s" % str(tweet_id)
                    break

                # set the next tweet id
                tweet_id = tw.in_reply_to_status_id

            # add a row to the data structure we'll write to a CSV file
            add_csv_row(already_seen, csv_data, excel_data, saved_tweets)

            # clean up
            del saved_tweets

    # twitter threw exception we can't recover from, write out what we can
    except TwitterUnrecoverableException:
        write_cached_tweets(tweets_to_cache)
        return

    # write out tweets to cache
    write_cached_tweets(tweets_to_cache)

    write_output_csv(csv_data["first"], csv_first_file, csv_headers)
    write_output_csv(csv_data["support"], csv_support_file, csv_headers)

    write_to_excel(excel_data, args.output)

    pprint(tweets_to_use)

    #if len(tweets_to_cache):
    #    with open(tweets_file, 'wb') as fp:
    #        json.dump(tweets_to_cache.values(), fp)

def time_between_tweets_in_hours(tw1, tw2):
    tweet1_time = parse(tw1.created_at)
    tweet2_time = parse(tw2.created_at)
    diff = tweet2_time - tweet1_time
    return "{:0.1f}".format(diff.total_seconds()/60/60)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Creates csv containing Gauge of Service data for tweets",
                                     version='%(prog)s 1.0')
    parser.add_argument("-id", "--status_id", type=int, help="Tweet ID")
    #parser.add_argument("-t", "--access_token", type=str, default="rDFfVkx9dIyfjdni3AUEnA", nargs="?",
    #                    help='Twitter access token')
    parser.add_argument("-o", "--output", type=str, help="output file [*.xlsx]")
    parser.add_argument("-i", "--input", type=str, help="input file [*.csv]")
    args = parser.parse_args()
    get_twitter_gos(args)

