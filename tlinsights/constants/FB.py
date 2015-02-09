# usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'cappy'

FB_PAGE_ID = "57851763662"
FB_AVG_SUPPORT_USER_ID = "100005475852648"

def fb_post_url_from_user_and_post_id(user_id, post_id):
    # concat("https://www.facebook.com/permalink.php?id=%(fb_page_id)s&v=wall&story_fbid=",
    #              substring(fbposts.postID FROM 13)) as Post, 57851763662_10152365093748663
    url = u"https://www.facebook.com/permalink.php?id={}&v=wall&story_fbid={}"
    return url.format(user_id, post_id[post_id.index("_") + 1:] )