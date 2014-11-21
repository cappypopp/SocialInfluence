#! /usr/bin/env python

__author__ = 'cappypopp'

import klout,argparse, sys

def get_klout_score_for(twitterName):

    key = "672g4ytq58vmpzjt5m89gatf"

    # create object with key
    k = klout.Klout(key)

    # get klout id for user
    try:
        kloutId = k.identity.klout(screenName=twitterName).get("id")

        #get score
        score = round(k.user.score(kloutId=kloutId).get("score"), 2)

        banner = "-"*len(twitterName)

        print "%s\n%s\n%s\nklout: %s\ninfluential in:" % (banner, twitterName, banner,score),

        topics = k.user.topics(kloutId=kloutId)
        influence = k.user.influence(kloutId=kloutId)

        infin = crack_influencers(influence["myInfluencers"])
        infout = crack_influencers(influence["myInfluencees"])
    except (klout.KloutError, klout.KloutHTTPError) as e:
        if e.e.code == 404:
            print "'%s' - user not found" % twitterName
            sys.exit(2)
        else:
            print e
            sys.exit(3)

    #[{u'displayName': u'Facebook', u'name': u'Facebook', u'imageUrl': u'http://kcdn3.klout.com/static/images/facebook-1365719248809.jpg', u'id': u'9052604650979936805', u'displayType': u'visible', u'topicType': u'entity', u'slug': u'facebook'}, {u'displayName': u'Social Media', u'name': u'Social Media', u'imageUrl': u'http://kcdn3.klout.com/static/images/topics/social-media2.png', u'id': u'8655841127676549162', u'displayType': u'visible', u'topicType': u'sub', u'slug': u'social-media'}, {u'displayName': u'Technology', u'name': u'Technology', u'imageUrl': u'http://kcdn3.klout.com/static/images/technology-1367972636528.jpg', u'id': u'5646379875885211920', u'displayType': u'visible', u'topicType': u'sub', u'slug': u'technology-sub'}]
    influential = [value for i in topics for (name, value) in i.iteritems() if name=="name"]
    print ", ".join([inf for inf in influential])

    print "influencers: %s" % ",".join([" %s(%s)" % (i['user'],i['score']) for i in infin]),
    print "\ninfluences: %s" % ",".join([" %s(%s)" % (i['user'],i['score']) for i in infout]),

def crack_influencers(influencers):
    res = []
    for inf in influencers:
        user = inf['entity']['payload']
        username = user['nick']
        kscore = round(user['score']['score'], 2)
        res.append({'user':username, 'score':kscore})
    return res

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Fetches Klout score using different network ids")
    parser.add_argument("twitterHandle", help="Twitter handle")
    args = parser.parse_args()
    get_klout_score_for(args.twitterHandle)

