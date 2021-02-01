# Code sourced from https://towardsdatascience.com/streaming-twitter-data-into-a-mysql-database-d62a02b050d6
# Used to prove my SQL server was able to receive Twitter data

from dateutil import parser
import json
import os
import tweepy
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# Set os paths and load .ENV file containing vars
project_folder = os.path.expanduser('~/PythonProjects/twitter_analysis-main')
load_dotenv(os.path.join(project_folder, 'twitterAuth.env'))

# Grabbing tokens and passwords from .twitterAuth.env file
access_token = os.environ.get("ACCESS_TOKEN")
access_token_secret = os.environ.get("ACCESS_TOKEN_SECRET")
consumer_key = os.environ.get("CONSUMER_KEY")
consumer_secret = os.environ.get("CONSUMER_SECRET")
password = os.environ.get("PASSWORD")


def connect(username, created_at, tweet, retweet_count, place, location):
    # Connecting to MySQL database
    try:
        con = mysql.connector.connect(host='localhost', database='twitterdb', user='root', password=password,
                                      charset='utf8')

        if con.is_connected():
            print("Successfully connected to database")
            # Insert Twitter data
            cursor = con.cursor()
            # twitter, golf
            query = "INSERT INTO tweets (username, created_at, tweet, retweet_count,place, location) VALUES (%s, %s, %s, %s, %s, %s)"
            cursor.execute(query, (username, created_at, tweet, retweet_count, place, location))
            con.commit()

    except Error as e:
        print(e)

    cursor.close()
    con.close()

    return


# Tweepy class for accessing Twitter API
class Streamlistener(tweepy.StreamListener):

    def on_connect(self):
        print("Successfully connected to Twitter API")

    def on_error(self):
        #if status_code != 200:
        print("Error found")
        # returning false disconnects the stream
        return False

    # Read in twitter JSON data and extract targets
    def on_data(self, data):

        try:
            raw_data = json.loads(data)

            if 'text' in raw_data:

                username = raw_data['user']['screen_name']
                created_at = parser.parse(raw_data['created_at'])
                tweet = raw_data['text']
                retweet_count = raw_data['retweet_count']

                if raw_data['place'] is not None:
                    place = raw_data['place']['country']
                    print(place)
                else:
                    place = None

                location = raw_data['user']['location']

                # Insert collected data into MySQL
                connect(username, created_at, tweet, retweet_count, place, location)
                print("Tweet colleted at: {} ".format(str(created_at)))
        except Error as e:
            print(e)


if __name__ == '__main__':
    # # #Allow user input
    # track = []
    # while True:

    # 	input1  = input("what do you want to collect tweets on?: ")
    # 	track.append(input1)

    # 	input2 = input("Do you wish to enter another word? y/n ")
    # 	if input2 == 'n' or input2 == 'N':
    # 		break

    # print("You want to search for {}".format(track))
    # print("Initialising Connection to Twitter API....")
    # time.sleep(2)

    # Authenticate for access
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    # create instance of Streamlistener
    listener = Streamlistener(api=api)
    stream = tweepy.Stream(auth, listener=listener)

    track = ['WSB', 'wallstreetbets', 'GME', '$GME', 'Gamestop']
    # track = ['nba', 'cavs', 'celtics', 'basketball']
    # choose what we want to filter by
    stream.filter(track=track, languages=['en'])
