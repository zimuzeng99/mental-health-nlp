import pandas as pd

subreddits = pd.read_csv('all_subreddits_list.csv')
subreddits = subreddits[subreddits['nsfw']=='nsfw=false']
subreddits = subreddits.drop(columns=['nsfw'])

mental_health_subreddits = pd.read_csv('mental_health_subreddits.csv')
mental_health_subreddits = mental_health_subreddits['subreddit'].tolist()

subreddits = subreddits[~subreddits['subreddit'].isin(mental_health_subreddits)]

subreddits = subreddits.head(500)

subreddits.to_csv('top_500_subreddits.csv', index = False, header=True)