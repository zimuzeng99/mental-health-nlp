# Script to scrape data from Reddit's mental health subreddits

from psaw import PushshiftAPI
import datetime as dt
import time
import json
import random
import pandas as pd

api = PushshiftAPI()

NUM_SECONDS_IN_DAY = 86400 # UNIX timestamp is in seconds so to advance timestamp by one day just have to
                            # increment timestamp by number of seconds in day
                            
def get_mental_health_data(subreddit, start_date, end_date):
    start_epoch=int(start_date.timestamp())
    end_epoch=start_epoch + NUM_SECONDS_IN_DAY
    
    submissions = []
    
    while end_epoch <= int(end_date.timestamp()):
        
        submissions.extend(list(api.search_submissions(after=start_epoch, before=end_epoch,
                                    subreddit=subreddit,
                                    filter=['id', 'title', 'score', 'selftext', 'link_flair_text', 'author', 'subreddit', 'create_utc'],
                                    limit=100)))
        
        start_epoch += NUM_SECONDS_IN_DAY
        end_epoch += NUM_SECONDS_IN_DAY
        
        print(len(submissions))
        
        time.sleep(3)
    
    submissions_json = list(map(lambda x: x[-1], submissions)) # get the dict from every submission
    submissions_json = list(filter(lambda x: x['author'] != '[deleted]', submissions_json)) # get rid of posts by deleted authors
    submissions_json = list(filter(lambda x: x['selftext'] not in ('[deleted]', '[removed]'), submissions_json)) # get rid of deleted posts
    submissions_json = list(filter(lambda x: len(x['selftext']) > 0, submissions_json)) # get rid of empty posts
    
    return submissions_json

def get_control_data(start_date, end_date):
    top_500_subreddits = pd.read_csv('top_500_subreddits.csv')
    top_500_subreddits = top_500_subreddits['subreddit'].tolist()

    control_data = []

    start_epoch = int(start_date.timestamp())
    end_epoch = start_epoch + NUM_SECONDS_IN_DAY

    while end_epoch <= int(end_date.timestamp()):
        subreddits = random.sample(top_500_subreddits, 5) # randomly choose 5 subreddits from top 500 subreddits, scrape 100 commments from each
        for subreddit in subreddits:
            comments = api.search_comments(subreddit=subreddit, after=start_epoch, before=end_epoch)
            i = 0
            for comment in comments:
                control_data.append(comment)
                i += 1
                
                if i >= 100:
                    break
            
            print(len(control_data))
            
            time.sleep(3)
        
        start_epoch += NUM_SECONDS_IN_DAY
        end_epoch += NUM_SECONDS_IN_DAY
    
    control_data_json = list(map(lambda x: x[-1], control_data)) # get the dict from every submission
    control_data_json = list(filter(lambda x: x['author'] != '[deleted]', control_data_json)) # get rid of posts by deleted authors
    control_data_json = list(filter(lambda x: x['body'] not in ('[deleted]', '[removed]'), control_data_json)) # get rid of deleted posts
    control_data_json = list(filter(lambda x: len(x['body']) > 0, control_data_json)) # get rid of empty posts
    
    for comment in control_data_json:
        comment['selftext'] = comment['body'] # change to selftext to make it consistent with data from mental health subreddits
        comment.pop('body', None)
        
    return control_data_json

control_data_json = []
control_data_json.extend(get_control_data(dt.datetime(2018, 1, 1), dt.datetime(2018, 5, 1)))
control_data_json.extend(get_control_data(dt.datetime(2018, 5, 1), dt.datetime(2018, 9, 1)))
control_data_json.extend(get_control_data(dt.datetime(2018, 9, 1), dt.datetime(2019, 1, 1)))
control_data_json.extend(get_control_data(dt.datetime(2019, 1, 1), dt.datetime(2019, 5, 1)))
control_data_json.extend(get_control_data(dt.datetime(2019, 5, 1), dt.datetime(2019, 12, 31)))

with open('raw_control_data.json', 'w') as fout:
    json.dump(control_data_json, fout)
    
depression_data_json = []
depression_data_json.extend(get_mental_health_data('depression', dt.datetime(2018, 1, 1), dt.datetime(2018, 5, 1)))
depression_data_json.extend(get_mental_health_data('depression', dt.datetime(2018, 5, 1), dt.datetime(2018, 9, 1)))
depression_data_json.extend(get_mental_health_data('depression', dt.datetime(2018, 9, 1), dt.datetime(2019, 1, 1)))
depression_data_json.extend(get_mental_health_data('depression', dt.datetime(2019, 1, 1), dt.datetime(2019, 5, 1)))
depression_data_json.extend(get_mental_health_data('depression', dt.datetime(2019, 5, 1), dt.datetime(2019, 12, 31)))

with open('raw_depression_data.json', 'w') as fout:
    json.dump(depression_data_json, fout)
    
anxiety_data_json = []
anxiety_data_json.extend(get_mental_health_data('Anxiety', dt.datetime(2018, 1, 1), dt.datetime(2018, 5, 1)))
anxiety_data_json.extend(get_mental_health_data('Anxiety', dt.datetime(2018, 5, 1), dt.datetime(2018, 9, 1)))
anxiety_data_json.extend(get_mental_health_data('Anxiety', dt.datetime(2018, 9, 1), dt.datetime(2019, 1, 1)))
anxiety_data_json.extend(get_mental_health_data('Anxiety', dt.datetime(2019, 1, 1), dt.datetime(2019, 5, 1)))
anxiety_data_json.extend(get_mental_health_data('Anxiety', dt.datetime(2019, 5, 1), dt.datetime(2019, 12, 31)))

with open('raw_anxiety_data.json', 'w') as fout:
    json.dump(anxiety_data_json, fout)

bipolar_data_json = []
bipolar_data_json.extend(get_mental_health_data('bipolar', dt.datetime(2018, 1, 1), dt.datetime(2018, 5, 1)))
bipolar_data_json.extend(get_mental_health_data('bipolar', dt.datetime(2018, 5, 1), dt.datetime(2018, 9, 1)))
bipolar_data_json.extend(get_mental_health_data('bipolar', dt.datetime(2018, 9, 1), dt.datetime(2019, 1, 1)))
bipolar_data_json.extend(get_mental_health_data('bipolar', dt.datetime(2019, 1, 1), dt.datetime(2019, 5, 1)))
bipolar_data_json.extend(get_mental_health_data('bipolar', dt.datetime(2019, 5, 1), dt.datetime(2019, 12, 31)))

with open('raw_bipolar_data.json', 'w') as fout:
    json.dump(bipolar_data_json, fout)

ocd_data_json = []
ocd_data_json.extend(get_mental_health_data('OCD', dt.datetime(2018, 1, 1), dt.datetime(2018, 5, 1)))
ocd_data_json.extend(get_mental_health_data('OCD', dt.datetime(2018, 5, 1), dt.datetime(2018, 9, 1)))
ocd_data_json.extend(get_mental_health_data('OCD', dt.datetime(2018, 9, 1), dt.datetime(2019, 1, 1)))
ocd_data_json.extend(get_mental_health_data('OCD', dt.datetime(2019, 1, 1), dt.datetime(2019, 5, 1)))
ocd_data_json.extend(get_mental_health_data('OCD', dt.datetime(2019, 5, 1), dt.datetime(2019, 12, 31)))

with open('raw_ocd_data.json', 'w') as fout:
    json.dump(ocd_data_json, fout)
    
bpd_data_json = []
bpd_data_json.extend(get_mental_health_data('BPD', dt.datetime(2018, 1, 1), dt.datetime(2018, 5, 1)))
bpd_data_json.extend(get_mental_health_data('BPD', dt.datetime(2018, 5, 1), dt.datetime(2018, 9, 1)))
bpd_data_json.extend(get_mental_health_data('BPD', dt.datetime(2018, 9, 1), dt.datetime(2019, 1, 1)))
bpd_data_json.extend(get_mental_health_data('BPD', dt.datetime(2019, 1, 1), dt.datetime(2019, 5, 1)))
bpd_data_json.extend(get_mental_health_data('BPD', dt.datetime(2019, 5, 1), dt.datetime(2019, 12, 31)))

with open('raw_bpd_data.json', 'w') as fout:
    json.dump(bpd_data_json, fout)

    

