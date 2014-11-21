__author__ = 'cappy'
def get_twitter_timeline():

    data_file = "%s/gos_data.json" % twitter_data_dir
    csv_file = "%s/gos_data.csv" % twitter_data_dir
    tweets_seen_file = "%s/tweets_seen.json" % twitter_data_dir

    make_sure_path_exists(twitter_data_dir)

    if os.path.isfile(tweets_seen_file):
        with open(tweets_seen_file, 'rb') as fp:
            tweets_seen = json.load(fp)
    else:
        tweets_seen = {}

    #tweets = []
    #tweets.append(api.GetStatus(527141793549123584)) # one response, no support
    tweets = [status for status in api.GetUserTimeline(trim_user=True,
                                                       count=50,
                                                       include_rts=False) if status.in_reply_to_status_id] # get our tweets that are responses

    processed = set() # sets allow constant time lookup

    csv_data = []

    for tweet in tweets:

        # reset this on each iteration because we may have
        # broken out of the previous iteration due to already
        # seeing a tweet in a conversation - GetUserTimeline()
        # returns all tweets and we could see multiple in a thread
        final = {"first_response": None,
                 "first_user": None,
                 "first_support": None}

        csv_headers = ["User Tweet Timestamp",
                       "User Tweet",
                       "User Tweet URL",
                       "First Touch Tweet",
                       "First Touch URL",
                       "First Touch Hours",
                       "First Support Tweet",
                       "First Support URL",
                       "First Support Hours"]

        tw = tweet # cache outer tweet for inner loop

        tweet_id = tw.id # cache id for inner loop

        already_seen = False # this is a tweet we've never seen before as far as we know

        while tweet_id:

            if tweet_id not in processed:

                processed.add(tweet_id) # store the tweet id so we don't process it again

                # if not the same as outer loop we need to fetch the next tweet
                if tweet_id != tweet.id:
                    tw = api.GetStatus(tweet_id, include_entities=False )
                    sleep(0.3) # play nice with the API to avoid rate limits

                url = tw.GetUrl()
                tweet_detail = tw.GetTweetDetail()

                if tw.user.id != avg_id: # if the tweet is from someone else
                    final["first_user"] = tw
                    csv_row["User Tweet"] = tweet_detail
                    csv_row["User Tweet URL"] = url
                else: # one of our tweets; we'll have to store data about it
                    #if search(r"\^JM|\^ZC|\^AH|#AVGSupport", tw.text):
                    if search(r"\^JM|#AVGSupport", tw.text):
                        final["first_support"] = tw
                        final["first_response"] = tw
                        csv_row["First Support URL"] = url
                        csv_row["First Touch URL"] = url
                        csv_row["First Support Tweet"] = tweet_detail
                        csv_row["First Touch Tweet"] = tweet_detail

                    else:
                        # we don't care about threads when we're at the root. This won't be a
                        # problem when running for real but will be in testing. We only care about
                        # tweets we make that are replies. This will make sure we don't falsely set
                        # our root tweet as our first response
                        if tw.in_reply_to_status_id:
                            final["first_response"] = tw
                            csv_row["First Touch Tweet"] = tweet_detail
                            csv_row["First Touch URL"] = url


                    # only store tweets from us - we'll use this to set the since_id and max_id
                    # params when we query twitter api to get tweets between two times
                    tweets_seen[tw.GetCreatedAtInSeconds()] = { "id": str(tw.id),
                                                                "url": url}
            else:
                already_seen = True
                break

            tweet_id = tw.in_reply_to_status_id

        if final["first_response"] and final["first_user"]:
            first_touch_time = time_between_tweets_in_hours(final["first_user"], final["first_response"])
            csv_row["User Tweet Timestamp"] = final["first_user"].GetCreatedAtInSeconds()
            print ("US >> %s\n"
                   "THEM >> %s\n"
                   "FIRST TOUCH >> %0.2f hours") % (final["first_response"].GetTweetDetail(),
                                                    final["first_user"].GetTweetDetail(),
                                                    first_touch_time)
            csv_row["First Touch Hours"] = first_touch_time
            if final["first_support"]:
                first_support_time = time_between_tweets_in_hours(final["first_user"], final["first_support"])
                print("SUPPORT >> %s\n"
                      "FIRST SUPPORT >> %0.2f hours") % (final["first_support"].GetTweetDetail(),
                                                         first_support_time)
                csv_row["First Support Hours"] = first_support_time
            csv_data.append(csv_row)
        elif already_seen:
            print "ALREADY PROCESSED >> %s" % tw.GetTweetDetail()
            already_seen = False
        else:
            print "UNKNOWN PATH"
        print "-----"

        del final # reset dictionary

        del csv_row # reset csv_row

        # write out the tweets we've seen
        with open(tweets_seen_file, 'wb') as fp:
                json.dump(tweets_seen, fp)

    with open(csv_file, 'wb') as fou:
        fou.write(u'\ufeff'.encode('utf8'))  # need this line to make Excel treat CSV as Unicode
        dw = DictUnicodeWriter(fou, fieldnames=csv_headers)
        dw.writeheader()
        for result_row in csv_data:
            dw.writerow(result_row)

    pprint(csv_data)

def get_search_timeline():

    search_term = quote("#AVGSupport OR ^JM OR ^AH OR ^ZC from:AVGFree")


    statuses = api.GetSearch(search_term,result_type="recent", until="2014-10-26")

    s = [t for t in statuses]
    for tweet in s:
        print "%s [%s][%s]" % (tweet.text, parse(tweet.created_at).strftime(time_fmt), tweet.in_reply_to_screen_name if tweet.in_reply_to_screen_name else "None")