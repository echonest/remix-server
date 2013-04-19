import os 
import ConfigParser
import urllib2
import json
import sys
import base64
import hmac, sha
import datetime
import time
import Queue
import threading
import re
import imp

import requests
import cherrypy
import boto
from boto.s3.key import Key
from boto.s3.connection import S3Connection

import db

expire_window = datetime.timedelta(hours=10)

CREDENTIALS = imp.load_source('YOUR_AMAZON_CREDENTIAL_FILE', 'PATH/TO/YOUR/CREDENTIAL/FILE')
AWS_ACCESS_KEY_ID=CREDENTIALS.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=CREDENTIALS.AWS_SECRET_ACCESS_KEY
API_KEY = 'YOUR_ECHO_NEST_API_KEY'
API_HOST = 'developer.echonest.com'

def to_json(data, callback):
    if callback is None:
        return json.dumps(data)
    else:
        return callback + '(' + json.dumps(data) + ')'

class Uploader(object):
    def __init__(self):
        self.num_workers = 10
        self.bucket = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).get_bucket('YOUR_S3_BUCKET')

        self.known_ids =  {}

        self.queue = Queue.Queue()
        self.proc_count = 0
        self.queue_count = 0
        self.tot_proc_time = 0
        self.avg_proc_time = 20
        self.proc_errors = 0
        self.qInfo = {}
        self.lock = threading.Lock()

        self.startWorkers()

        db.load()

    # Returns info about the process queue
    def qinfo(self, callback=None, _=''):
        if callback:
            cherrypy.response.headers['Content-Type']= 'text/javascript'
        else:
            cherrypy.response.headers['Content-Type']= 'application/json'
        size = self.queue.qsize()

        results = { 'qlength' : size, 'estimated_wait': self.get_wait_time(size), 
                'queued' : self.queue_count, 
                'processed' : self.proc_count, 
                'errors' : self.proc_errors, 
                'avg_proc_time' : int(self.avg_proc_time) }

        return to_json(results, callback)
    qinfo.exposed = True

    # Return an Amazon policy that allows clients to upload audio to static.echonest.com on Amazon S3
    def verify(self, callback=None, v=None, _=''):
        if callback:
            cherrypy.response.headers['Content-Type']= 'text/javascript'
        else:
            cherrypy.response.headers['Content-Type']= 'application/json'

        expiration_time = (datetime.datetime.utcnow() + expire_window).isoformat() + 'Z'

        if v == "audio":
            policy_document = {
                "expiration": expiration_time,
                "conditions": [ 
                    {"bucket": "YOUR_S3_BUCKET"}, 
                    ["starts-with", "$key", "YOUR/S3/PATH"],
                    {"acl": "public-read"},
                     ["starts-with", "$success_action_redirect", "http://"],
                     ["starts-with", "$Content-Type", "audio/mpeg"],
                     ["content-length-range", 0, 40000000] 
                ]
            }

        js_policy = json.dumps(policy_document)
        policy = base64.b64encode(js_policy)
        signature = base64.b64encode( hmac.new(AWS_SECRET_ACCESS_KEY, policy, sha).digest())

        results = {
            'policy' : policy,
            'full_policy' : js_policy,
            'signature' : signature,
            'key' : AWS_ACCESS_KEY_ID
        }
        return to_json(results, callback)
    verify.exposed = True

    def profile(self, trid, callback=None, _=''):
        if callback:
            cherrypy.response.headers['Content-Type']= 'text/javascript'
        else:
            cherrypy.response.headers['Content-Type']= 'application/json'

        url = db.get_url(trid);
        if url == None:
            ready = False
            url = ''
        else:
            ready = True
        
        results = { 'status' : ready, 'url' : url }
        return to_json(results, callback)
    profile.exposed = True


    # Put the url in the queue to be analyzed.
    def qanalyze(self, url, api_key, tag='', callback=None, _=''):
        if callback:
            cherrypy.response.headers['Content-Type']= 'text/javascript'
        else:
            cherrypy.response.headers['Content-Type']= 'application/json'

        if not url in self.qInfo:
            ti = { 'status' : 'pending', 'url' : url, 'tag' : tag, 'api_key': api_key, 'queue_count' : self.get_queue_count()  }
            self.update_wait_info(ti)
            self.qInfo[url] = ti
            self.queue.put(ti)
        else:
            ti = self.qInfo[url]
            self.update_wait_info(ti)

        try:
            temp = {}
            temp.update(ti)
            return to_json(temp, callback)
        except RuntimeError:
            print "That Runtime Error (dictionary changed size during iteration) again"
            return to_json(temp, callback)

    qanalyze.exposed = True

    # Get the number of items in the queue
    def get_queue_count(self):
        try:
            self.lock.acquire()
            self.queue_count += 1
            count = self.queue_count
        finally:
            self.lock.release()
        return count

    # Start the queue
    def startWorkers(self):
        for i in xrange(self.num_workers):
            t = threading.Thread(target=self.real_worker, args=(i,))
            t.daemon = True
            t.start()

    # What we do to things in the queue
    def real_worker(self, which):
        while True: 
            ti = self.queue.get()
            start_time = time.time()
            self.lock.acquire()
            url = ti['url']
            tag = ti['tag']
            ti['start_time'] = time.time()
            self.lock.release()

            try:
                # Upload the track for analysis
                ti['status'] = 'transferring'
                code, message, track = uploader(ti['url'], ti['tag'], ti['api_key'])
                ti['message'] = message
                if code == 0 and track:
                    trid = track['id']
                    ti['status'] = 'analyzing'
                    code, message = check_profile(trid, ti['api_key'])
                    ti['message'] = message
                    if code == 0:
                        ti['trid'] = trid 

                        # Add the file to the database
                        db.add_file(trid, track['artist'], track['title'], tag, url)
                        ti['status'] = 'done'

                    else:
                        ti['status'] = 'error'
                else:
                    ti['status'] = 'error'
            except Exception as e:
                ti['message'] = 'internal error'
                ti['status'] = 'error'
                error_log(ti['url'])

            delta_time = time.time() - start_time
            self.lock.acquire()
            self.proc_count += 1
            self.tot_proc_time += delta_time
            self.avg_proc_time = self.tot_proc_time / self.proc_count
            if ti['status'] == 'error':
                self.proc_errors += 1
            self.lock.release()

    # Update the estimated time until the queue item is done
    def update_wait_info(self, ti):
        position_in_queue = ti['queue_count'] - self.proc_count
        if position_in_queue < 0:
            position_in_queue = 0
        ti['position_in_queue'] = position_in_queue
        if ti['status'] == 'done':
            ti['estimated_wait'] = 0
        elif ti['status'] == 'pending':
            ti['estimated_wait'] = self.get_wait_time(position_in_queue)
        else:
            cur_proc_time = time.time() - ti['start_time']
            remaining = self.avg_proc_time - cur_proc_time
            if remaining < 5:
                remaining = 5
            ti['estimated_wait'] = int(remaining)

    # Get the wait time
    def get_wait_time(self, pos):
        avg_proc_time = self.avg_proc_time
        return int((pos * avg_proc_time) / self.num_workers + avg_proc_time)

 
# Get the results from uploading a track to the Analyzer
def uploader(url, tag, api_key):
    results = upload(url, api_key)
    code = results['response']['status']['code'] 
    message = results['response']['status']['message']
    if code == 0:
        track = results['response']['track']
    else:
        track = None
        error_log('error uploading track ' + url + ' ' + message)
    return code, message, track

# Upload a track to the Analyzer
def upload(url, api_key):
    kwargs = {'url':url, 'format':'json', 'api_key':api_key, 'wait':'false'}
    dot = url.rindex('.')
    if False and dot >=0:
        filetype = url[dot+1:]
        kwargs['filetype'] = filetype
    headers = { 'content-type' : 'application/octet-stream' }
    req = requests.post("http://" + API_HOST + "/api/v4/track/upload", params=kwargs, headers=headers, data='body')
    results = json.loads(req.text)
    return results


# Check that the analysis is done
def check_profile(trid, api_key, timeout=100):
    delay = 3
    code = -1
    message = 'timeout waiting for analysis'
    kwargs = {'id':trid, 'format':'json', 'api_key':api_key, 'bucket':['audio_summary']}
    for i in xrange(0, timeout, delay):
        results = requests.get("http://" + API_HOST + "/api/v4/track/profile", params=kwargs)
        results = json.loads(results.text)
        if is_done(results):
            code = results['response']['status']['code']
            message = results['response']['status']['message']
            break
        time.sleep(delay)
    return code, message

# Check the results
def is_done(results):
    if results['response']['status']['code'] == 0:
        track = results['response']['track']
        if track['status'] == 'complete':
            return True
        if track['status'] == 'error':
            return True
    return False

def error_log(msg):
    print 'ERROR: ' + msg
    out = open('error.log', 'a')
    print >>out,  msg
    out.close()



if __name__ == '__main__':
    conf_path = os.path.abspath('web.conf')
    print 'reading config from', conf_path
    cherrypy.config.update(conf_path)

    config = ConfigParser.ConfigParser()
    config.read(conf_path)
    production_mode = config.getboolean('settings', 'production')

    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Set up site-wide config first so we get a log if errors occur.
    if production_mode:
        print "Starting in production mode"
        cherrypy.config.update({'environment': 'production',
                                'log.error_file': 'simdemo.log',
                                'log.screen': True})
    else:
        print "Starting in development mode"
        cherrypy.config.update({'noenvironment': 'production',
                                'log.error_file': 'site.log',
                                'log.screen': True})

    cherrypy.quickstart(Uploader(), '/Uploader')

