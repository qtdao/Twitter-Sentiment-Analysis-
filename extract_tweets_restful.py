import tweepy
from kafka import KafkaClient, SimpleProducer
import argparse
import json
import config

def get_parser():
    """Get parser for command line arguments."""
    parser = argparse.ArgumentParser(description="Twitter Downloader")
    parser.add_argument("-q",
                        "--query",
                        dest="query",
                        help="Query/Filter",
                        default='-')
    parser.add_argument("-l",
                        "--limit",
                        dest="limit",
                        help="Maximum number of tweets we want to collect",
                        default=10000)
    return parser

#Setup kafka
client = KafkaClient("localhost:9092")
producer = SimpleProducer(client)




#Pass our consumer key and consumer secret to Tweepy's user authentication handler
#We can use AppAuthHandler instead to increase the limit
auth = tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
auth.set_access_token(config.access_key, config.access_secret)
api = tweepy.API(auth, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)

#Switching to application authentication, ie. AppAuthHandler for higher limit
#increase from 180 to 450
auth = tweepy.AppAuthHandler(config.consumer_key, config.consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)


#Error handling
if (not api):
    print ("Problem connecting to API")

#Check how many queries you have left using rate_limit_status() method
print(api.rate_limit_status()['resources']['search'])



#Take the arguement from parser
parser = get_parser()
args = parser.parse_args()

searchQuery = args.query

print('Im searching for ', searchQuery)

maxTweets = int(args.limit) #Maximum number of tweets we want to collect
tweetsPerQry = 100 #this is the max that API permits

#Starting tweet count
tweetCount = 0

#Tell the Cursor method that we want to use the Search API (api.search)
#Also tell Cursor our query, and the maximum number of tweets to return
for tweet in tweepy.Cursor(api.search, q=searchQuery).items(maxTweets) :
    
    #tweet will be in JSON format
    #Write the JSON format to the text file, and add one to the number of tweets we've collected
    #f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
    #print(tweet._json)
    producer.send_messages('twitterstream', bytes(json.dumps(tweet._json), 'ascii'))
    tweetCount += 1

#Display how many tweets we have collected
    print("Downloaded {0} tweets".format(tweetCount), end = '\r')

#Send tweets to kafka
#producer.send('twitterstream', tweet)

#Check how many queries you have left using rate_limit_status() method
print(api.rate_limit_status()['resources']['search'])

''' Trend API '''
#world_trends = api.trends_available(_woeid=1)
#_woeid is Yahoo! Where on earth ID

# Get all (it's always 10) trending topics in San Francisco (its WOEID is 2487956)
#sfo_trends = twitter.trends.place(_id = 2487956)

#print json.dumps(sfo_trends, indent=4))

''' User API'''
# Get a list of followers of a particular user
#twitter.followers.ids(screen_name="cocoweixu")
# Get a particular user's timeline (up to 3,200 of his/her most recent tweets)
#twitter.statuses.user_timeline(screen_name="billybob")

''' Extracting missing original twitter from retwiter '''
#For extracting the missing original tweets from retweets, think of the following pseudo-code.
#Store all downloaded tweets in a set (say set A)
#From this set filter out the retweets & extract the original tweet from these retweets (say set B)
#Insert in set A all unique tweets from set B that are not already in set A
