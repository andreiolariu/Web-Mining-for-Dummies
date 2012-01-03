import logging, time
                                                 
from lib.pipeline import Pipeline

logging.basicConfig(
	filename='execution.log',
	format='%(asctime)-6s: %(name)s - %(levelname)s - %(message)s',
	level=logging.DEBUG)
logger = logging.getLogger('ipad.py')
logger.debug('Starting the monitor')

p = Pipeline(['ipad3', '"ipad 3"', '"new ipad"', 'ipad2s', '"ipad 2s"', 'ipad2', '"ipad 2"', 'ipadmini', '"ipad mini"'], savefile='ipaddata', savedelta=3600)

def strip_useless_info(input_queue=None, output_queue=None, keep_monitoring=None):
    #... because less is more
    # also because it's faster to check out the results in the console
    # you can get the original tweet back using
    # http://api.twitter.com/1/statuses/show/tweet_id.json
    keep = ['coordinate_prob', 'coordinates', 'created_at', 'id', 'text', \
            'geo', 'from_user_id']
    while keep_monitoring():
        while input_queue.qsize():
            tweet = input_queue.get()
            new_tweet = {key: tweet[key] for key in keep if key in tweet}
            new_tweet['text'] = new_tweet['text'].lower()
            output_queue.put(new_tweet)
        time.sleep(1)
        
p.add_function(strip_useless_info, {})

p.run()


