import urllib, urllib2

import simplejson as json

def fetch_url(url, get=None, post=None):
    user_agent = 'Andrei Olariu\'s Web Mining for Dummies'
    headers = {'User-Agent': user_agent}
    if get:
        data = urllib.urlencode(get)
        url = "%s?%s" % (url, data)
    req = urllib2.Request(url, post, headers)
    try:
        response = urllib2.urlopen(req).read()
        response = json.loads(response)
    except Exception, e:
        print 'error in reading %s: %s' % (url, e)
        return None
    return response
    

        
