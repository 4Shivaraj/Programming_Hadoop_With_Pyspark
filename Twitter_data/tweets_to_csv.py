"""
    @Author: Shivaraj
    @Date: 22-10-2022
    @Last Modified by: Shivaraj
    @Last Modified date: 22-10-2022
    @Title: 1. Use the real time twitter data.
            2. Save in the DB.
            3. Preprocess the data (Use Python + Spark).
            4. Store the cleaning data on HDFS.
            5. Store the result in HDFS.
            6. Dump data into a centralised data store (Azure BLOB storage) using Azure Data Factory for further
                analysis in data pipeline.
"""
import sys
import re
import tweepy
import configparser
import pandas as pd
import json
from data_log import get_logger

lg = get_logger('twitter data from tweepy','data_log.log')

class StreamingDataToCsv:
    def __init__(self, my_dict:dict):
        self.api_key = my_dict.get('api_key')
        self.api_key_secret = my_dict.get('api_key_secret')
        self.access_token = my_dict.get('access_token')
        self.access_token_secret = my_dict.get('access_token_secret')
        self.api = my_dict.get('api')
        self.query = my_dict.get('query')
        self.max_tweets = my_dict.get('max_tweets')

    def authentication(self):
        """
        Description:
            This function is used to authenticate Twitter api.
        Parameter:
            None.
        Return:
            Returns api
        """
        try:
            auth = tweepy.OAuthHandler(self.api_key, self.api_key_secret)
            auth.set_access_token(self.access_token, self.access_token_secret)
            api = tweepy.API(auth, wait_on_rate_limit=True)
            return api
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")

    def fetching_tweets(self):
        """
        Description:
            This function is used to fetching tweets from Twitter api.
        Parameter:
            None.
        Return:
            Returns tweet_dataset
        """
        try:
            searched_tweets = [status for status in tweepy.Cursor(self.api.search_tweets, q=self.query).items(self.max_tweets)]
            my_list_of_dicts = []
            for each_json_tweet in searched_tweets:
                my_list_of_dicts.append(each_json_tweet._json)

            with open('tweet_json_Data.txt', 'w') as file:
                file.write(json.dumps(my_list_of_dicts, indent=4))

            my_demo_list = []
            with open('tweet_json_Data.txt', encoding='utf-8') as json_file:
                all_data = json.load(json_file)
                for each_dictionary in all_data:
                    tweet_id = each_dictionary['user']['id']
                    name = each_dictionary['user']['name']
                    tweet = each_dictionary['text']
                    follower = each_dictionary['user']['followers_count']
                    friends = each_dictionary['user']['friends_count']
                    verified = each_dictionary['user']['verified']
                    post = each_dictionary['user']['statuses_count']
                    profile_image = each_dictionary['user']['profile_image_url']
                    location = each_dictionary['user']['location']
                    my_demo_list.append({'tweet_id': float(tweet_id),
                                         'name': str(name),
                                         'tweets': str(tweet),
                                         'followers': int(follower),
                                         'friends': int(friends),
                                         'verified': bool(verified),
                                         'total_post': str(post),
                                         'profile_image': str(profile_image),
                                         'location': str(location)
                                         })
            tweet_dataset = pd.DataFrame(my_demo_list,
                                         columns=['tweet_id', 'name', 'tweets', 'followers', 'friends', 'verified',
                                                  'total_post', 'profile_image', 'location'])
            return tweet_dataset
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")

    def preprocess_tweet(self, tweet):
        """
       Description:
           This function is used to clean tweets using regex patterns.
       Parameter:
           None.
       Return:
           Returns cleaned tweets
       """
        try:
            text = tweet.lower()
            text1 = re.sub('rt @\w+: ', " ", text)
            text2 = re.sub('(@[A-Za-z0-9]+)|([^0-9a-zA-Z \t])|(\w+.\/\/\S+)', " ", text1)
            text3 = re.sub(r"\s+[a-zA-Z]\s+", ' ', text2)
            text4 = re.sub(r'\s+', ' ', text3)
            emoji_pattern = re.compile(pattern="["
                                                u"\U0001F600-\U0001F64F"  # emoticons
                                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                                u"\U00002500-\U00002BEF"  # chinese char
                                                u"\U00002702-\U000027B0"
                                                u"\U00002702-\U000027B0"
                                                u"\U000024C2-\U0001F251"
                                                u"\U0001f926-\U0001f937"
                                                u"\U00010000-\U0010ffff"
                                                u"\u2640-\u2642"
                                                u"\u2600-\u2B55"
                                                u"\u200d"
                                                u"\u23cf"
                                                u"\u23e9"
                                                u"\u231a"
                                                u"\ufe0f"  # dingbats
                                                u"\u3030"
                                                "]+", flags=re.UNICODE)
            return emoji_pattern.sub(r'', text4)
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")

    def clean_tweet_dataset(self):
        """
       Description:
           This function is used to appending clean tweets into dataframe.
       Parameter:
           None.
       Return:
           Returns tweet_dataset
       """
        try:
            tweet_dataset = self.fetching_tweets()
            cleaned_tweets = []
            for tweet in tweet_dataset['tweets']:
                clean_tweet = self.preprocess_tweet(tweet)
                cleaned_tweets.append(clean_tweet)

            tweet_dataset['tweets'] = pd.DataFrame(cleaned_tweets)
            print(tweet_dataset.head())
            return tweet_dataset
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")

    def saving_to_csv(self):
        """
       Description:
           This function is used to saving tweets dataset to local directory.
       Parameter:
           None.
       Return:
           Returns None.
       """
        try:
            tweet_dataset = self.clean_tweet_dataset()
            # Writing dataset to csv file
            tweet_dataset.to_csv("D:/DataEngg/CFP_Assignment/Twitter_streaming/tweet_data1.csv")
        except Exception:
            exception_type, _, exception_traceback = sys.exc_info()
            line_number = exception_traceback.tb_lineno
            lg.exception(
                f"Exception type : {exception_type} \nError on line number : {line_number}")

def main():
    try:
        # Read all the credentials from config files
        config = configparser.ConfigParser()
        config.read('config.ini')
        api_key = config['twitter']['api_key']
        api_key_secret = config['twitter']['api_key_secret']
        access_token = config['twitter']['access_token']
        access_token_secret = config['twitter']['access_token_secret']

        # Authentication
        auth = tweepy.OAuthHandler(api_key, api_key_secret)
        auth.set_access_token(access_token, access_token_secret)
        api = tweepy.API(auth, wait_on_rate_limit=True)

        # Defining Search keyword and number of tweets and searching tweets
        query = 'COVID-19 lang:en'
        max_tweets = 200
        my_dict = {'api_key':api_key, 'api_key_secret':api_key_secret, 'access_token':access_token,
                   'access_token_secret':access_token_secret, 'api':api,'query':query,'max_tweets':max_tweets }
        return my_dict
    except Exception:
        exception_type, _, exception_traceback = sys.exc_info()
        line_number = exception_traceback.tb_lineno
        lg.exception(
            f"Exception type : {exception_type} \nError on line number : {line_number}")

if __name__ == '__main__':
    my_dict = main()
    tweet_obj = StreamingDataToCsv(my_dict)
    tweet_obj.saving_to_csv()






