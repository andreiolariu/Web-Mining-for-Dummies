import threading, Queue, time, logging

import simplejson as json

from lib.api.twitter import fetch_tweets
        
logger = logging.getLogger('pipeline')

class Pipeline:
    
    def __init__(self, keywords, interval=10, per_interval=100, \
            savefile=None, savedelta=3600):
        self.functions = []
        self.interval = interval
        self.per_interval = per_interval
        self.functions.append((self.monitor_twitter, {'keywords': keywords}))
        self.savefile = savefile
        self.savedelta = savedelta
        
    def add_function(self, function, params):
        self.functions.append((function, params))
        
    def get_interval(self, l):
        if self.per_interval == 0:
            return self.interval
        if l < 0.4 * self.per_interval:
            self.interval += 2
        elif l < 0.6 * self.per_interval:
            self.interval += 1
        elif l == self.per_interval:
            self.interval -= 2
        elif l > 0.8 * self.per_interval:
            self.interval -= 1
        self.interval = max(self.interval, 1)
        return self.interval
    
    def keep_monitoring(self):
        return self.running
        
    def stop(self):
        self.running = False
    
    def run(self):
        self.running = True
        old_queue = Queue.Queue()
        self.functions[0][1]['queue'] = old_queue
        threading.Thread(target=self.functions[0][0], \
                kwargs=self.functions[0][1]).start()
        for function in self.functions[1:]:
            new_queue = Queue.Queue()
            function[1]['input_queue'] = old_queue
            function[1]['output_queue'] = new_queue
            function[1]['keep_monitoring'] = self.keep_monitoring
            threading.Thread(target=function[0], kwargs=function[1]).start()
            old_queue = new_queue
        if self.savefile:
            threading.Thread(target=self.writer, kwargs={'queue': old_queue}).start()
        else:
            self.output = old_queue
        
    def monitor_twitter(self, queue=None, keywords=None):
        next_run = time.time()
        id_cache = set([])
        while self.keep_monitoring():
            if next_run <= time.time():
                batch = fetch_tweets(keywords)
                batch = [t for t in batch if t['id'] not in id_cache]
                logger.debug('Fetched %s tweets' % len(batch))
                id_cache.update([t['id'] for t in batch])
                for t in batch:
                    queue.put(t)
                next_run += self.get_interval(len(batch)) 
            time.sleep(1)
            
    def writer(self, queue=None):
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
            
