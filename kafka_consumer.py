from kafka import KafkaConsumer
import pandas as pd
import json
import pandas as pd
import nltk
import sqlite3 as lite
import re

#import textprocessing

def initialize():
    '''
        Initialize a function that takes postgres database name as arguent and sets up a pandas dataframe.
        Connection to postgres database is established using psycopg2 postgres connector and cursor is initiated to run
        SQL queries. Function returns, the tweets dataframe, postgres connector and the SQL cursor as objects.
        '''
    tweets = pd.DataFrame() # set-up an pandas dataframe
    # set-up a postgres connection
    conn = None
    try:
        conn = lite.connect('/Users/tuandao/datascience/tweetdb.db')
        
        dbcur = conn.cursor()
    
        with open('create_tweets_table.sql', 'r') as s:
            sql_script = s.read()
            conn.executescript(sql_script)
            conn.commit()


        dbcur.execute('SELECT * FROM tweets')
    
        data = dbcur.fetchone()
    
        print("SQLite version; %s" % data)

    except lite.Error as e:
        print("Error %s:" % e.args[0])
        #sys.exit(1)

    print("connection successful")
    return (tweets, conn, dbcur)


def extracttweetfeatures(tweets,output):
    '''
        extracttweetfeatures function takes tweets dataframe and a output list as input. Output list comprises of the list
        of all tweets in a json format consumed by json consumer. Function theb extracts the important features such as
        tweet text, movie name, language, country, user name, coordinates, location, retweets count.
        '''
    try:
        tweets['text'] = list(map(lambda tweet: tweet['text'], output))
    except IndexError:
        tweets['text'] = 'Data Error'
    except KeyError:
        tweets['text'] = 'Data Error'

    try:
        tweets['lang'] = list(map(lambda tweet: tweet['user']['lang'], output))
    except IndexError:
        tweets['lang'] = 'Data Error'
    except KeyError:
        tweets['text'] = 'Data Error'

    try:
        tweets['country'] = list(map(lambda tweet: tweet['place']['country'] if tweet['place'] != None else None, output))
    except IndexError:
        tweets['country'] = 'Data Error'
    except KeyError:
        tweets['text'] = 'Data Error'

    try:
        tweets['user_nm'] = list(map(lambda tweet: tweet['user']['name'].encode('utf-8'), output))
    except IndexError:
        tweets['user_nm'] = 'Data Error'
    except KeyError:
        tweets['text'] = 'Data Error'
    
    try:
        tweets['screen_nm'] = list(map(lambda tweet: tweet['user']['screen_name'].encode('utf-8'), output))
    except IndexError:
        tweets['screen_nm'] = 'Data Error'
    except KeyError:
        tweets['text'] = 'Data Error'

    try:
        tweets['coordinates_lat'] = list(map(lambda tweet: str(tweet['coordinates']['coordinates'][1]) if tweet['coordinates'] != None else None, output))
    except IndexError:
        tweets['coordinates_lat'] = 'Not available'
    except KeyError:
        tweets['coordinates_lat'] = 'Not available'
    except TypeError:
        tweets['coordinates_lat'] = 'Not available'

    try:
        tweets['coordinates_long'] = list(map(lambda tweet: str(tweet['coordinates']['coordinates'][0]) if tweet['coordinates'] != None else None , output))
    except IndexError:
        tweets['coordinates_long'] = 'Not available'
    except KeyError:
        tweets['coordinates_long'] = 'Not available'
    except TypeError:
        tweets['coordinates_long'] = 'Not available'
    
    try:
        tweets['location'] = list(map(lambda tweet: tweet['user']['location'] if tweet['user'] != None else None, output))
    except IndexError:
        tweets['location'] = 'Data Error'
    except KeyError:
        tweets['text'] = 'Data Error'

    try:
        tweets['retweets_count'] = list(map(lambda tweet: tweet['retweeted_status']['retweet_count'], output))
    except IndexError:
        tweets['retweets_count'] = 0
    except KeyError:
        tweets['retweets_count'] = 0

    try:
        tweets['followers_count'] = list(map(lambda tweet: tweet['user']['followers_count'], output))
    except IndexError:
        tweets['followers_count'] = 0
    except KeyError:
        tweets['followers_count'] = 0

    try:
        tweets['favourites_count'] = list(map(lambda tweet: tweet['user']['favourites_count'], output))
    except IndexError:
        tweets['favourites_count'] = 0
    except KeyError:
        tweets['favourites_count'] = 0

    try:
        tweets['friends_count'] = list(map(lambda tweet: tweet['user']['friends_count'], output))
    except IndexError:
        tweets['friends_count'] = 0
    except KeyError:
        tweets['friends_count'] = 0

    try:
        tweets['created_at'] = list(map(lambda tweet: tweet['created_at'], output))
    except IndexError:
        tweets['created_at'] = 'Data Error'
    except KeyError:
        tweets['created_at'] = 'Data Error'


def preprocess(s, lowercase=False, remove = False):
    
    emoticons_str = r"""
        (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
        )"""
    
    regex_str = [
                 emoticons_str,
                 r'<[^>]+>', # HTML tags
                 r'(?:@[\w_]+)', # @-mentions
                 r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
                 r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
                 
                 r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
                 r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
                 r'(?:[\w_]+)', # other words
                 r'(?:\S)' # anything else
                 ]
        
    remove_regex_str = [
                        emoticons_str,
                        r'<[^>]+>', # HTML tags
                        r'(?:@[\w_]+)', # @-mentions
                        r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
                        r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',  # URLs
                        r'[^\w\s]' # alphanumerical character and space
                        ]
            
    #If remove is True, then HTML tag, @mention, #hashtag, and URL, and emoji
    #will be remove from the string
    if remove:
        tokens_re = re.compile(r'('+'|'.join(remove_regex_str)+')', re.VERBOSE | re.IGNORECASE)
        tokens = tokens_re.sub('', s)
    #Else: Return a splited string will regex_str condition
    else:
        tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
        tokens = tokens_re.findall(s)

    emoticon_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
    
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens

def hashtag_mention(string):
    '''
       hashtag_mention function taks tweets dataframe, then
       find all hasgtag and mention (ie. @) in the text, and add them a seperate column
       output is a string
    '''
    regex_str = [
                 r'(?:@[\w_]+)', # @-mentions
                 r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"
                 ] # hash-tags
        
    tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
                 
    tokens = tokens_re.findall(string)
    return ' '.join(tokens)


def calculatesentiments(pdtweets):
    '''
        calculatesentiments function takes tweets dataframe. Function then uses vader lexicon to compute the sentiment
        scores for all the tweets. Further, the scores are then used to classify tweets as positive, negative and neutral.
        '''
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    sentence = pdtweets['text_clean'].values[0]
    
    #Sample foranalyzer.polarity_scores(sentence)
    #{'pos': 0.746, 'compound': 0.8316, 'neu': 0.254, 'neg': 0.0}
    
    sentiment_score = analyzer.polarity_scores(sentence)['compound']
    pdtweets['sentiment_score'] = sentiment_score

    if sentiment_score > .1:
        pdtweets['sentiment'] = 'positive'
    elif sentiment_score <-.1:
        pdtweets['sentiment'] = 'negative'
    else:
        pdtweets['sentiment'] = 'neutral'



def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    '''

    # set-up a Kafka consumer
    consumer = KafkaConsumer('twitterstream',
                             bootstrap_servers = ['localhost:9092'] #default setup
                             )
    pdtweets,conn, dbcur = initialize()
    
    for msg in consumer:
        tweet_data = []
        tweet_data.append(json.loads(msg.value.decode('utf-8')))

        extracttweetfeatures(pdtweets, tweet_data)

        #Add clean text to pd
        #This can only process 1 line at the time, bottleneck

        pdtweets['text_clean'] = preprocess(pdtweets['text'].values[0], remove = True)
        
        #Calculate sentiment score using VADER
        #This can only process 1 line at the time, bottleneck
        calculatesentiments(pdtweets)
        
        #add hashtag_name to pd
        pdtweets['hashtag_mention'] = hashtag_mention(pdtweets['text'].values[0])
        
        #This will automatically committed
        pdtweets.to_sql('tweets', conn, if_exists = 'append', index = False)
        
        #pdtweets.set_option('display.max_colwidth', 10000)
        print(pdtweets['text'].values[0])
        print(pdtweets['text_clean'].values[0])
        print('\n')
        
        '''
        #msg value and key are raw bytes -- decode if nessessary!
        #e.g., for unicode: `message.value.decode('utf-8')
        print ("%s:%d:%d: key=%s text=%s" % (msg.topic, msg.partition,
                                             msg.offset, msg.key,
                                             msg.value))

        '''
#If this file is ran directly from the shell, __name__ = __main__
#If it is called/imported  from somewhere else __name__ will be this file'name
if __name__ == "__main__":
  main()
