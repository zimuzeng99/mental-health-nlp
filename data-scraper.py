# Script to scrape data from Reddit's mental health subreddits

from psaw import PushshiftAPI
import datetime as dt
import time
import json

api = PushshiftAPI()

NUM_SECONDS_IN_DAY = 86400 # UNIX timestamp is in seconds so to advance timestamp by one day just have to
                            # increment timestamp by number of seconds in day
                            
def get_data(subreddit, start_date, end_date):
    start_epoch=int(start_date.timestamp())
    end_epoch=start_epoch + NUM_SECONDS_IN_DAY
    
    submissions = []
    
    while True:
        if end_epoch > end_date.timestamp():
            break
        
        submissions.extend(list(api.search_submissions(after=start_epoch, before=end_epoch,
                                    subreddit=subreddit,
                                    filter=['id', 'title', 'score', 'selftext', 'link_flair_text', 'author', 'subreddit', 'create_utc'],
                                    limit=100)))
        
        start_epoch += NUM_SECONDS_IN_DAY
        end_epoch += NUM_SECONDS_IN_DAY
        
        time.sleep(3)
    
    submissions_json = list(map(lambda x: x[-1], submissions)) # get the dict from every submission
    submissions_json = list(filter(lambda x: x['author'] != '[deleted]', submissions_json)) # get rid of posts by deleted authors
    submissions_json = list(filter(lambda x: x['selftext'] not in ('[deleted]', '[removed]'), submissions_json)) # get rid of deleted posts
    
    raw_data_file_name = 'raw_data_' + subreddit + '.json'
    with open(raw_data_file_name, 'w') as fout:
        json.dump(submissions_json, fout)
        
        
get_data('depression', dt.datetime(2018, 1, 1), dt.datetime(2018, 1, 5))
        



    

