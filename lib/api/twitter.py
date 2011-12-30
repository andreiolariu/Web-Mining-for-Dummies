from lib.helper import fetch_url

def fetch_tweets(keywords):
    url = 'http://search.twitter.com/search.json'
    values = {
            'count' : 100,
            'q' : ' OR '.join(keywords) + ' -rt',
            'rpp': 100,
            'page': 1,
            'result_type': 'recent',
            'with_twitter_user_id': 'true',
            'lang': 'en',
    }
    response = fetch_url(url, values)
    if response and 'results' in response:
        return response['results']
    else:
        return []
