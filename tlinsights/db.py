# -*- coding: utf-8 -*-
__author__ = 'cappy'

from sys import exit
from datetime import datetime
from functools import wraps
from twitter import Status
import logging
import twitter
import MySQLdb as mdb
import MySQLdb.cursors as cursors

from constants import DB, TWITTER, FB


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the console handler
formatter = logging.Formatter('%(name)s[%(lineno)d]: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

class TLInsightsDB(object):

    # very simple enum type
    QUERY_BASE, QUERY_GOS, QUERY_FULL = range(3)

    def __init__(self, db_name=DB.DB_NAME):
        try:
            self.cxn = mdb.connect(DB.DB_SERVER, DB.DB_USER, DB.DB_PASS, db_name, use_unicode=True,
                                   charset=DB.DB_CHARSET, cursorclass=cursors.DictCursor)
            self.cur = self.cxn.cursor()
        except mdb.MySQLError as err:
            logger.error("Error: {:d} {}".format(err.args[0], err.args[1]))
            exit(err.args[0])

    def _query_tag_decorator(tags):  # used to pass args to decorator (decorator generator)
        def _query_decorator(func):  # decorator
            @wraps(func)  # to keep __doc__, __name__, and __module__ pointing to the original function
            def decorated_query(self, *args):  # decorated function to return
                try:
                    result = func(self, *args)
                    return result
                except mdb.MySQLError as e:
                    logger.error("Error: {:d} {}".format(e.args[0], e.args[1]))
                    logger.debug(self.cur._last_executed)
            return decorated_query
        return _query_decorator

    @_query_tag_decorator("cursor closed or invalid")
    def _build_gos_query_results(self, query_type):
        result = []
        while True:

            tmp = []

            row = self.cur.fetchone()

            if row is not None:

                tmp.append(row["postID"])
                tmp.append(row["Post"])
                tmp.append(row["PostDate"])
                tmp.append(row["PostMessage"])

                if query_type > self.QUERY_BASE:
                    tmp.append(row["ReplyDate"])
                    tmp.append(row["ReplyMessage"])
                    tmp.append(row["GOS"])

                if query_type > self.QUERY_GOS:
                    tmp.append(row["Zach"])
                    tmp.append(row["Aiyman"])
                    tmp.append(row["Esc"])
                    tmp.append(row["CZ"])

                result.append(tmp)
            else:
                break
        return result

    @_query_tag_decorator("tweet not found")
    def get_tweet_by_id(self, tweet_id):
        with self.cxn:
            q = "SELECT p.*, " \
                "u.id as user_id," \
                "u.name as user_name," \
                "u.screen_name as user_screen_name," \
                "u.created_at as user_created," \
                "u.url as user_url," \
                "u.description as user_description," \
                "u.statuses_count as user_statuses_count," \
                "u.favourites_count as user_favourites_count," \
                "u.followers_count as user_followers_count," \
                "u.friends_count as user_friends_count," \
                "u.listed_count as user_listed_count," \
                "u.location as user_location," \
                "u.time_zone as user_time_zone," \
                "u.lang as user_lang," \
                "u.protected as user_protected," \
                "u.verified as user_verified," \
                "u.geo_enabled as user_geo_enabled " \
                " from twposts p, twusers u " \
                "WHERE p.id = %s AND p.user_id = u.id"
            q = "SELECT * from twposts WHERE id = %s"
            self.cur.execute(q, (tweet_id,))  # has to be a tuple, thus the ending comma
            result = self.cur.fetchone()
            if result is not None:
                tw = twitter.Status.NewFromJsonDict(result)
                #TODO: migrate this code!
                tw.SetCreatedAt(self._convert_time_from_mysql_to_twitter(tw.created_at))
                q = "SELECT * from twusers WHERE id = %s"
                self.cur.execute(q, (result["user_id"],))
                result = self.cur.fetchone()
                if result is not None:
                    user = twitter.User.NewFromJsonDict(result)
                    if user is not None:
                        user.SetCreatedAt(self._convert_time_from_mysql_to_twitter(user.created_at))
                        tw.SetUser(user)
                        result = tw
        return result

    @_query_tag_decorator("tweet not found")
    def get_tweet_by_id_raw(self, tweet_id):
        with self.cxn:
            q = "SELECT p.*, " \
                "u.id as user_id," \
                "u.name as user_name," \
                "u.screen_name as user_screen_name," \
                "u.created_at as user_created_at," \
                "u.url as user_url," \
                "u.description as user_description," \
                "u.statuses_count as user_statuses_count," \
                "u.favourites_count as user_favourites_count," \
                "u.followers_count as user_followers_count," \
                "u.friends_count as user_friends_count," \
                "u.listed_count as user_listed_count," \
                "u.location as user_location," \
                "u.time_zone as user_time_zone," \
                "u.lang as user_lang," \
                "u.protected as user_protected," \
                "u.verified as user_verified," \
                "u.geo_enabled as user_geo_enabled " \
                " from twposts p, twusers u " \
                "WHERE p.id = %s AND p.user_id = u.id"
            self.cur.execute(q, (tweet_id,))  # has to be a tuple, thus the ending comma
            result = self.cur.fetchone()
        return result

    @_query_tag_decorator("tweet not found")
    def get_user_by_id(self, user_id):
        with self.cxn:
            q = "SELECT * from twusers WHERE id = %s"
            self.cur.execute(q, (user_id,))  # has to be a tuple, thus the ending comma
            result = self.cur.fetchone()
        return result


    @_query_tag_decorator("can't save tweet")
    def save_tweet(self, tweet):

        result = None

        self.save_user(tweet.GetUser())

        q = """INSERT INTO twposts (id,
                                    user_id,
                                    created_at,
                                    text,
                                    retweeted,
                                    retweet_count,
                                    favorited,
                                    favorite_count,
                                    in_reply_to_status_id,
                                    in_reply_to_user_id,
                                    in_reply_to_screen_name,
                                    lang,
                                    truncated,
                                    possibly_sensitive,
                                    unanswered)
                    VALUES (%s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s)
                    ON DUPLICATE KEY UPDATE
                      text = VALUES(text),
                      retweeted = VALUES(retweeted),
                      retweet_count = VALUES(retweet_count),
                      favorited = VALUES(favorited),
                      favorite_count = VALUES(favorite_count),
                      possibly_sensitive = VALUES(possibly_sensitive),
                      unanswered = VALUES(unanswered);
                    """

        with self.cxn:
            arg_tuple = (
                tweet.id,
                tweet.user.id,
                self._convert_time_from_twitter_to_mysql(tweet.created_at),
                self._decode_if_string(tweet.text),
                tweet.retweeted,
                tweet.retweet_count,
                tweet.favorited,
                tweet.favorite_count,
                tweet.in_reply_to_status_id,
                tweet.in_reply_to_user_id,
                tweet.in_reply_to_screen_name,
                self._decode_if_string(tweet.lang),
                tweet.truncated,
                tweet.possibly_sensitive,
                tweet.unanswered
            )

            result = self.cur.execute(q, arg_tuple)

        return result


    def save_tweets(self, tweets):
        for tw in tweets:
            self.save_tweet(tw)

    @_query_tag_decorator("can't save 404 tweet")
    def save_404_tweet(self, id):

        with self.cxn:
            q = "INSERT IGNORE INTO tw404posts(id) VALUES (%s)"
            self.cur.execute(q, (id,))

    def save_404_tweets(self, tweets):
        for tw in tweets:
            self.save_404_tweet(tw)

    def is_404_tweet(self, id):
        with self.cxn:
            q = "SELECT COUNT(*) as dead_tweet FROM tw404posts WHERE id = %s"
            self.cur.execute(q, (id,))
            result = self.cur.fetchone()
            return 'dead_tweet' in result and result['dead_tweet'] == 1

    @_query_tag_decorator("can't save user")
    def save_user(self, twitter_user):

        with self.cxn:

            q = """INSERT INTO twusers (
                                    id,
                                    name,
                                    created_at,
                                    screen_name,
                                    url,
                                    description,
                                    statuses_count,
                                    favourites_count,
                                    followers_count,
                                    friends_count,
                                    listed_count,
                                    location,
                                    time_zone,
                                    lang,
                                    protected,
                                    verified,
                                    geo_enabled
                                   )
                            VALUES (
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s)
                            ON DUPLICATE KEY UPDATE
                                    name = VALUES(name),
                                    created_at = VALUES(created_at),
                                    screen_name = VALUES(screen_name),
                                    url = VALUES(url),
                                    description = VALUES(description),
                                    statuses_count = VALUES(statuses_count),
                                    favourites_count = VALUES(favourites_count),
                                    followers_count = VALUES(followers_count),
                                    friends_count = VALUES(friends_count),
                                    listed_count = VALUES(listed_count),
                                    location = VALUES(location),
                                    time_zone = VALUES(time_zone),
                                    lang = VALUES(lang),
                                    protected = VALUES(protected),
                                    verified = VALUES(verified),
                                    geo_enabled = VALUES(geo_enabled);"""

            arg_tuple = (twitter_user.id,
                         self._decode_if_string(twitter_user.name),
                         self._convert_time_from_twitter_to_mysql(twitter_user.created_at),
                         self._decode_if_string(twitter_user.screen_name),
                         twitter_user.url,
                         self._decode_if_string(twitter_user.description),
                         twitter_user.statuses_count,
                         twitter_user.favourites_count,
                         twitter_user.followers_count,
                         twitter_user.friends_count,
                         twitter_user.listed_count,
                         self._decode_if_string(twitter_user.location),
                         self._decode_if_string(twitter_user.time_zone),
                         self._decode_if_string(twitter_user.lang),
                         twitter_user.protected,
                         twitter_user.verified,
                         twitter_user.geo_enabled)

            result = self.cur.execute(q, arg_tuple)

            return result
        
    @_query_tag_decorator("can't save user")
    def save_user_from_dict(self, twitter_user):

        with self.cxn:

            q = """INSERT INTO twusers (
                                    id,
                                    name,
                                    created_at,
                                    screen_name,
                                    url,
                                    description,
                                    statuses_count,
                                    favourites_count,
                                    followers_count,
                                    friends_count,
                                    listed_count,
                                    location,
                                    time_zone,
                                    lang,
                                    protected,
                                    verified,
                                    geo_enabled
                                   )
                            VALUES (
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s)
                            ON DUPLICATE KEY UPDATE
                                    name = VALUES(name),
                                    created_at = VALUES(created_at),
                                    screen_name = VALUES(screen_name),
                                    url = VALUES(url),
                                    description = VALUES(description),
                                    statuses_count = VALUES(statuses_count),
                                    favourites_count = VALUES(favourites_count),
                                    followers_count = VALUES(followers_count),
                                    friends_count = VALUES(friends_count),
                                    listed_count = VALUES(listed_count),
                                    location = VALUES(location),
                                    time_zone = VALUES(time_zone),
                                    lang = VALUES(lang),
                                    protected = VALUES(protected),
                                    verified = VALUES(verified),
                                    geo_enabled = VALUES(geo_enabled);"""

            arg_tuple = (long(self._check_and_decode_val(twitter_user, 'id')),
                         self._check_and_decode_val(twitter_user, 'name'),
                         self._convert_time_from_twitter_to_mysql(
                             self._check_and_decode_val(twitter_user, 'created_at')),
                         self._check_and_decode_val(twitter_user, 'screen_name'),
                         self._check_and_decode_val(twitter_user, 'url'),
                         self._check_and_decode_val(twitter_user, 'description'),
                         self._check_and_decode_val(twitter_user, 'statuses_count'),
                         self._check_and_decode_val(twitter_user, 'favourites_count'),
                         self._check_and_decode_val(twitter_user, 'followers_count'),
                         self._check_and_decode_val(twitter_user, 'friends_count'),
                         self._check_and_decode_val(twitter_user, 'listed_count'),
                         self._check_and_decode_val(twitter_user, 'location'),
                         self._check_and_decode_val(twitter_user, 'time_zone'),
                         self._check_and_decode_val(twitter_user, 'lang'),
                         self._check_and_decode_val(twitter_user, 'protected'),
                         self._check_and_decode_val(twitter_user, 'verified'),
                         self._check_and_decode_val(twitter_user, 'geo_enabled'))

            result = self.cur.execute(q, arg_tuple)

            return result

    @_query_tag_decorator("FB all posts not found")
    def get_all_fb_posts(self):

            q = """SELECT fbposts.postID,
                   concat("https://www.facebook.com/permalink.php?id=%(fb_page_id)s&v=wall&story_fbid=",
                      substring(fbposts.postID FROM 13)) as Post,
                   fbposts.date as PostDate,
                   fbposts.message as PostMessage
                   from fbposts
                      where fbposts.pageID = %(fb_page_id)s
                      and (fbposts.date between '2014-10-01 00:00:00' and NOW())
                      and fbposts.fromUserID != %(fb_page_id)s
                      order by fbposts.date
                      ASC;"""

            arg_dict = {
                "fb_page_id": FB.FB_PAGE_ID
            }

            result = []
            with self.cxn:
                records = self.cur.execute(q, arg_dict)
                if records > 0:
                    result = self._build_gos_query_results(self.QUERY_BASE)
            return result

    @_query_tag_decorator("FB first touch posts not found")
    def get_fb_first_touch_posts(self):
        q = """select fbposts.postID,
               concat("https://www.facebook.com/permalink.php?id=%(fb_page_id)s&v=wall&story_fbid=",
                  substring(fbposts.postID FROM 13)) as Post,
               fbposts.date as PostDate,
               fbposts.message as PostMessage,
               MIN(fbcomments.date) as ReplyDate,
               fbcomments.message as ReplyMessage,
               TIMESTAMPDIFF(Hour, MIN(fbcomments.date), fbposts.date)*-1 as GOS
                  from fbcomments
                  join fbposts on fbcomments.parentPostID = fbposts.postID
                  where fbcomments.pageID = %(fb_page_id)s
                  and fbposts.pageID = %(fb_page_id)s
                  and (fbposts.date between "2014-10-01 00:00:00" and NOW())
                  and (fbcomments.fromUserID = %(fb_page_id)s or fbcomments.fromUserID =%(fb_user_id)s)
                  and fbposts.fromUserID != %(fb_page_id)s
                  group by fbposts.date
                  order by fbposts.date
                  ASC;"""

        arg_dict = {
            "fb_page_id": FB.FB_PAGE_ID,
            "fb_user_id": FB.FB_AVG_SUPPORT_USER_ID
        }

        result = []
        with self.cxn:
            records = self.cur.execute(q, arg_dict)
            if records > 0:
                result = self._build_gos_query_results(self.QUERY_GOS)
        return result

    @_query_tag_decorator("FB unanswered posts not found")
    def get_fb_unanswered_posts(self):
        q = """select fbposts.postID,
               concat("https://www.facebook.com/permalink.php?id=%(fb_page_id)s&v=wall&story_fbid=",
                  substring(fbposts.postID FROM 13)) as Post,
               fbposts.date as PostDate,
               fbposts.message as PostMessage
               from fbposts where pageID = %(fb_page_id)s
               and fromUserID != %(fb_page_id)s
               and (date between "2014-10-01 00:00:00" and NOW())
               and postID not in (
                  select distinct parentPostID
                  from fbcomments
                  where ( fbcomments.fromUserID = %(fb_page_id)s or fbcomments.fromUserID =%(fb_user_id)s)
                  and (date between "2014-10-01 00:00:00" and NOW())
                  order by date
                  desc)
               order by date
               ASC;"""

        arg_dict = {
            "fb_page_id": FB.FB_PAGE_ID,
            "fb_user_id": FB.FB_AVG_SUPPORT_USER_ID
        }

        result = []
        with self.cxn:
            records = self.cur.execute(q, arg_dict)
            if records > 0:
                result = self._build_gos_query_results(self.QUERY_BASE)
        return result

    @_query_tag_decorator("FB support posts not found")
    def get_fb_support_first_touch_posts(self):

        q = """select fbposts.postID, concat("https://www.facebook.com/permalink.php?id=%(fb_page_id)s&v=wall&story_fbid=",
                  substring(fbposts.postID FROM 13)) as Post,
               fbposts.date as PostDate,
               fbposts.message as PostMessage,
               MIN(fbcomments.date) as ReplyDate,
               fbcomments.message as ReplyMessage,
               TIMESTAMPDIFF(Hour, MIN(fbcomments.date), fbposts.date) * -1 as GOS,
	           IF(fbcomments.message LIKE "%(zach)s" = 1,"Y","N") as Zach,
	           IF(fbcomments.message LIKE "%(aiyman)s" = 1,"Y","N") as Aiyman,
	           IF(fbcomments.message REGEXP BINARY "#AVGSupport" = 1,"Y","N") as Esc,
	           IF(fbcomments.message REGEXP BINARY "AVG Customer Care" = 1,"Y","N") as CZ,
	           fbcomments.fromUserID as replyAccount
	           from fbcomments
               join fbposts ON fbcomments.parentPostID = fbposts.postID
               where fbcomments.pageID = %(fb_page_id)s
               and fbposts.pageID = %(fb_page_id)s
		       and fbposts.fromUserID != %(fb_page_id)s
               and (fbposts.date between '2014-10-01 00:00:00' and NOW())
		       #All the responses are support responses
               and (
				#either from the page as an escalation or as a support reply
				(fbcomments.fromUserID = %(fb_page_id)s
					and (
						fbcomments.message REGEXP BINARY "#AVGSupport"
						or
						fbcomments.message LIKE "%(aiyman)s"
						or
						fbcomments.message LIKE "%(zach)s"
						or
						fbcomments.message REGEXP BINARY "AVG Customer Care"
					)
				)
				or fbcomments.fromUserID = %(fb_user_id)s
		       )
               group by fbposts.date
               order by fbposts.date
               ASC;"""

        arg_dict = {
            "fb_page_id": FB.FB_PAGE_ID,
            "fb_user_id": FB.FB_AVG_SUPPORT_USER_ID,
            "zach": "%^ZCS%",
            "aiyman": "%^AHS%"
        }

        result = []

        with self.cxn:
            records = self.cur.execute(q, arg_dict)
            if records > 0:
                result = self._build_gos_query_results(self.QUERY_FULL)

        return result

    @classmethod
    def _check_and_decode_val(cls, d, k):
        v = d.get(k, None)
        result = cls._decode_if_string(v)
        return result

    @classmethod
    def _decode_if_string(cls, val):
        # originally had basestring in the isinstance call but this caused unicode strings to call decode("utf8") as well
        return val.decode("utf8") if isinstance(val, str) else val

    @_query_tag_decorator("can't save gos")
    def save_gos_interaction(self, gos_dict):

        with self.cxn:
            # odd - the format string here is NOT REALLY A NORMAL FORMAT STRING. YOU MUST USE %s for
            # all fields.
            # See http://stackoverflow.com/questions/5785154/python-mysqldb-issues-typeerror-d-format-a-number-is-required-not-str
            q = """INSERT IGNORE INTO twgos(user_tweet_id,
                                            user_tweet_url,
                                            user_tweet_date,
                                            user_tweet_text,
                                            support_tweet_id,
                                            support_tweet_date,
                                            support_tweet_text,
                                            support_gos,
                                            gos_type)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE ( support_gos = VALUES(support_gos));"""
            arg_tuple = (long(gos_dict['PostId']),
                         gos_dict['Post'],
                         self._convert_time_from_twitter_to_mysql(gos_dict['PostDate']),
                         gos_dict['PostMessage'],
                         long(gos_dict['ReplyPostId']),
                         self._convert_time_from_twitter_to_mysql(gos_dict['ReplyDate']),
                         gos_dict['ReplyMessage'],
                         gos_dict['GOS'],
                         gos_dict['GOSType'])
            result = self.cur.execute(q, arg_tuple)
            return result

    @classmethod
    def _convert_time_from_twitter_to_mysql(cls, str_twitter_date):
        # time data 'Wed Nov 02 12:51:23 +0000 2011' does not match format '%m/%d/%Y %I:%M:%S %p'
        dt = datetime.strptime(str_twitter_date, TWITTER.TWITTER_API_TIME_FORMAT)
        newdt = dt.strftime(DB.DB_TIME_FORMAT)
        return newdt

    @classmethod
    def _convert_time_from_mysql_to_twitter(cls, dt):
        newdt = dt.strftime(TWITTER.TWITTER_API_TIME_FORMAT)
        return newdt


if __name__ == '__main__':
    json_user = r"""{
   "lang":"en",
   "favorited":true,
   "truncated":false,
   "text":".@JudyatAVG: Fever Pitch: Live Final is inspiration to us all http://t.co/54zBwzzPPW - @AVGFree @AVGBusiness #AVG #thepitch14",
   "created_at":"Tue Oct 28 10:22:28 +0000 2014",
   "retweeted":false,
   "source":"<a href=\"https://dev.twitter.com/docs/tfw\" rel=\"nofollow\">Twitter for Websites</a>",
   "user":{
      "id":403394026,
      "profile_sidebar_fill_color":"DDEEF6",
      "profile_text_color":"333333",
      "followers_count":185,
      "location":"Global",
      "profile_background_color":"233A5C",
      "listed_count":3,
      "utc_offset":-28800,
      "statuses_count":1241,
      "description":"Follow us for news, insight, ideas and practical know-how for growing your MSP business",
      "friends_count":101,
      "profile_link_color":"0084B4",
      "profile_image_url":"https://pbs.twimg.com/profile_images/2319098754/zfwde97dby5193r1iyt8_normal.jpeg",
      "profile_banner_url":"https://pbs.twimg.com/profile_banners/403394026/1414157293",
      "profile_background_image_url":"http://pbs.twimg.com/profile_background_images/778165478/6dd52db7f8bfeb0646bf1f39bbfabad7.jpeg",
      "screen_name":"AVGBusiness",
      "lang":"en",
      "profile_background_tile":false,
      "favourites_count":1,
      "name":"AVG Business",
      "url":"http://t.co/PxBfV9h5ds",
      "created_at":"Wed Nov 02 12:51:23 +0000 2011",
      "time_zone":"Pacific Time (US & Canada)",
      "protected":false
   },
   "retweet_count":2,
   "id":527042709786079233,
   "favorite_count":1
}"""
    import json

    tweet_json = json.loads(json_user)
    tweet = Status.NewFromJsonDict(tweet_json)
    db = TLInsightsDB(DB.DB_TEST_NAME)
    db.save_tweet(tweet)
    db.get_tweet_by_id(123)