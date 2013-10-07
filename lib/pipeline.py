import threading, Queue, time, logging, socket

import simplejson as json
from twython import TwythonStreamer

from settings import *

logger = logging.getLogger('pipeline')

class Pipeline:
    '''
        Defines a pipeline for polling an API and processing and saving the 
        results
        At this time it only works on twitter. Will expand (perhaps) in time
    '''
    
    def __init__(self, keywords=[], locations=None, interval=10, 
    		per_interval=100, savefile=None, savedelta=3600):
        self.functions = []
        self.interval = interval
        self.per_interval = per_interval
        self.functions.append((self.monitor_twitter_stream, 
        		{'keywords': keywords, 'locations': locations}))
        self.savefile = savefile
        self.savedelta = savedelta

	# Change timeout
	socket.setdefaulttimeout(10)
        
    def add_function(self, function, params):
        # Add a 'processing' function to the pipeline
        # The function must accept the parameters: input_queue, output_queue
        # and keep_monitoring
        # Others parameters can be added in the params dictionary
        self.functions.append((function, params))
        
    def get_interval(self, l):
        # Adapt the polling interval for the fetching function
        if self.per_interval == 0:
            return self.interval
        if l < 0.3 * self.per_interval:
            self.interval *= 1.4
        elif l < 0.5 * self.per_interval:
            self.interval *= 1.2
        elif l > 0.8 * self.per_interval:
            self.interval *= 0.5
        elif l > 0.65 * self.per_interval:
            self.interval *= 0.8
        self.interval = int(max(self.interval, 25))
        self.interval = min(self.interval, 300)
        return 10 # :) self.interval
    
    def keep_monitoring(self):
        # See stop() for info
        return self.running
        
    def stop(self):
        # Stops the pipeline
        # For use when in ipython
        self.running = False
        self.stream.disconnect()
    
    def run(self):
        # Start the pipeline threads
        self.running = True
        
        # Execute the 'fetcher' functions
        old_queue = Queue.Queue()
        self.functions[0][1]['queue'] = old_queue
        threading.Thread(
                target=self.capsule, 
                args=(self.functions[0][0], self.functions[0][1])
        ).start()
        
        # Execute the 'processing' functions
        for function in self.functions[1:]:
            new_queue = Queue.Queue()
            function[1]['input_queue'] = old_queue
            function[1]['output_queue'] = new_queue
            function[1]['keep_monitoring'] = self.keep_monitoring
            threading.Thread(
                    target=self.capsule, 
                    args=(function[0], function[1])
            ).start()
            old_queue = new_queue
            
        # Execute the 'writer' function, if requested
        # If not, return the output queue
        if self.savefile:
            threading.Thread(
                    target=self.capsule, 
                    args=(self.writer, {'queue': old_queue})
            ).start()
        else:
            self.output = old_queue
        
    def monitor_twitter_stream(self, queue=None, keywords=None, locations=None):
        # Fetcher function, for monitoring twitter

        class MyStreamer(TwythonStreamer):
          count = 0
          log_time = time.time()
          id_cache = set([])

          def on_success(self, tweet):
            if 'text' in tweet and 'id' in tweet:
              if tweet['id'] not in self.id_cache:
                self.id_cache.add(tweet['id'])
                queue.put(tweet)
                self.count += 1

            if self.log_time + 60 < time.time():
              self.log_time = time.time()
              logger.debug('Fetched %s tweets' % self.count)
              self.count = 0

          def on_error(self, status_code, data):
            logger.debug('Error in Streamer: %s' % status_code)

        self.stream = MyStreamer(
            CONSUMER_KEY, CONSUMER_SECRET,
            ACCESS_TOKEN, ACCESS_TOKEN_SECRET
        )

        if keywords:
          self.stream.statuses.filter(keywords=keywords)
        else:
          self.stream.statuses.filter(locations=locations)
            
    def writer(self, queue=None):
        # Write processed data to file
        next_run = time.time() + self.savedelta
        while self.keep_monitoring():
            if next_run <= time.time():
                tweets = []
                while queue.qsize():
                    tweets.append(queue.get())
                f = open(self.savefile, 'a+')
                f.write(json.dumps(tweets))
                f.write('\n')
                f.close()
                logger.debug('Wrote %s tweets' % len(tweets))
                next_run += self.savedelta
            time.sleep(1)
            
    def capsule(self, function, kwargs):
        # Encapsulates functions, to catch errors and restart them
        while True:
            try:
                function(**kwargs)
                break
            except Exception, e:
                logger.debug('Error in %s: %s - using %s' % (function, e, kwargs))
                time.sleep(4)
                
