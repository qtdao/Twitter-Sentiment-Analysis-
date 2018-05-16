import tweepy
from kafka import SimpleProducer, KafkaClient
import json
import argparse
import config

def get_parser():
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(description="Twitter Downloader")
    parser.add_argument("-q",
                        "--query",
                        dest="query",
                        help="Query/Filter",
                        default='-')
    return parser



class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twitter stream and push it to Kafka"""

    def __init__(self, api):
    
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client, async = True)
    
        #batch_send_every_n = 1000
        #batch_send_every_t = 10
    def on_data(self, data):
        '''This methos id called whenever new data arrives. New data will be in str type'''
        try:
            #Write new data to our storage
            #f.write(data)
            
            #Convert data into bytes type then push it to kafka sever
            #It must be converted before pushing
            self.producer.send_messages('twitterstream', bytes(data, 'ascii'))

            #Open dat file to write data to
            #f = open('/Users/tuandao/datascience/Twitter/myfile_streaming.json','w')
            #The file needs to be closed afterward
            
            #data = json.loads(data)
            #json.dump(data, f)
            #f.write('\n')
            return True
        except Exception as e:
            print(e)
            return False
        return True
    '''
    def on_status(self, status):
        """ This method is called whenever new status arrives from live stream.
        We must then convert status into byte type before send it to kafka topic"""
        msg =  status.text.encode('utf-8')
        print(type(stat))
        print(type(msg))
        try:
            self.producer.send_messages('twitterstream', msg)
        except Exception as e:
            print(e)
            return False
        return True
        '''

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream

if __name__ == '__main__':
    #API Credential
    consumer_key = config.consumer_key
    consumer_secret = config.consumer_secret
    access_key = config.access_key
    access_secret = config.access_secret

    # Create Auth object
    '''Query speed can be improved by using AppAuthHandler instead
        The rate will be increased from 18k to 25k'''
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    #Take the arguement from parser
    parser = get_parser()
    args = parser.parse_args()
    searchQuery = args.query
    
    print('Im listening to ', searchQuery)
    
    # Create stream and bind the listener to it
    stream = tweepy.Stream(auth, listener = TweeterStreamListener(api))

    #Custom Filter rules pull all traffic for those filters in real time.
    #track can taka list as input i.e. [a, b]
    stream.filter(track = [searchQuery], languages = ['en'])
    #stream.filter(locations=[-180,-90,180,90], languages = ['en'])
