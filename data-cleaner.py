import json
from scipy import stats

with open('purified_bpd_data.json') as f:
    raw_data = json.load(f)
    
import re
import numpy as np

""" Data cleaning pipeline
1. Replace all URLs with empty string using regular expression, since URLs can confuse the model
2. Split posts into paragraphs so that posts have more consistent length, also individual paragraphs are more similar to
social media posts in other platforms like Twitter where people just periodically post snippets of their life rather than 
long detailed posts, so more generalisable to other social media
3. Get rid of bottom 5% length and top 5% length so that posts have more consistent length, posts too short aren't informative,
posts too long cause padding to be necessary
4. Get rid of special characters from posts, since they add no value to text understanding and introduce noise
5. Get rid of unnecessary fields in each JSON object to save storage space
"""

def remove_urls(raw_data):
    cleaned_data = []
    for submission in raw_data:
        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', submission['selftext'])
        if len(urls) == 0:
            cleaned_data.append(submission)
    
    return cleaned_data

def split_paragraphs(raw_data):
    cleaned_data = []
    for submission in raw_data:
        paragraphs = submission['selftext'].split('\n\n')
        for i in range(0, len(paragraphs)):
            new_post = dict(submission)
            new_post['selftext'] = paragraphs[i]
            new_post['id'] = new_post['id'] + '/' + str(i)
            cleaned_data.append(new_post)
            
    return cleaned_data

def remove_short_and_long_posts(raw_data):
    post_lengths = []
    for submission in raw_data:
        post_lengths.append(len(submission['selftext']))
    
    post_lengths = list(filter(lambda x: x > 0, post_lengths)) # get rid of empty posts before applying 5% cutoff
    post_lengths = np.array(post_lengths)
    
    low_cutoff = 50
    high_cutoff = 1000
    
    print(stats.percentileofscore(post_lengths, low_cutoff))
    print(stats.percentileofscore(post_lengths, high_cutoff))
    
    cleaned_data = []
    for submission in raw_data:
        if len(submission['selftext']) > low_cutoff and len(submission['selftext']) < high_cutoff:
            cleaned_data.append(submission)
            
    return cleaned_data

# function to remove special characters
def remove_special_characters(raw_data):
    # define the pattern to keep
    pat = r'[^a-zA-z0-9.,!?/:;\"\'\s]' 
    
    for submission in raw_data:
        submission['selftext'] = re.sub(pat, '', submission['selftext'])
        submission['selftext'] = submission['selftext'].replace('\n', ' ')
        submission['selftext'] = submission['selftext'].replace('\t', ' ')
        
    return raw_data

def drop_unnecessary_fields(raw_data):
    fields_to_drop = ['author_created_utc', 'author_flair_css_class', 'author_flair_text', 
                      'author_fullname', 'can_gild', 'controversiality', 'created', 'distinguished',
                      'gilded', 'is_submitter', 'link_id', 'nest_level', 'parent_id',
                      'permalink', 'reply_delay', 'retrieved_on', 'stickied', 'subreddit_id',
                      'subreddit_type']
    
    for submission in raw_data:
        for key in fields_to_drop:
            submission.pop(key, None)
    
    return raw_data

cleaned_data = remove_urls(raw_data)
cleaned_data = split_paragraphs(cleaned_data)
cleaned_data = remove_short_and_long_posts(cleaned_data)
cleaned_data = remove_special_characters(cleaned_data)
cleaned_data = drop_unnecessary_fields(cleaned_data)

with open('cleaned_bpd_data.json', 'w') as fout:
    json.dump(cleaned_data, fout)