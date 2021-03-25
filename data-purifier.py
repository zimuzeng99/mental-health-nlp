"""
The point of the data purifier is to take the raw data that has been scraped from Reddit,
and identify users who have posted in multiple subreddits indicating that they suffer from multiple mental
disorders. Posts by these users should be filtered out since they do not represent a specific mental disorder."""

import json

with open('raw_depression_data.json') as f:
    raw_depression_data = json.load(f)
    
with open('raw_anxiety_data.json') as f:
    raw_anxiety_data = json.load(f)

with open('raw_ocd_data.json') as f:
    raw_ocd_data = json.load(f)

with open('raw_bipolar_data.json') as f:
    raw_bipolar_data = json.load(f)

with open('raw_bpd_data.json') as f:
    raw_bpd_data = json.load(f)

with open('raw_control_data.json') as f:
    raw_control_data = json.load(f)
    
depression_users = list(set(map(lambda x: x['author'], raw_depression_data)))
anxiety_users = list(set(map(lambda x: x['author'], raw_anxiety_data)))
ocd_users = list(set(map(lambda x: x['author'], raw_ocd_data)))
bipolar_users = list(set(map(lambda x: x['author'], raw_bipolar_data)))
bpd_users = list(set(map(lambda x: x['author'], raw_bpd_data)))
control_users = list(set(map(lambda x: x['author'], raw_control_data)))

all_users = depression_users + anxiety_users + ocd_users + bipolar_users + bpd_users + control_users

user_frequencies = {}
for user in all_users:
    if user not in user_frequencies:
        user_frequencies[user] = 1
    else:
        user_frequencies[user] += 1

duplicate_users = []
for user in user_frequencies:
    if user_frequencies[user] > 1:
        duplicate_users.append(user)
duplicate_users = set(duplicate_users)

purified_depression_data = list(filter(lambda x: x['author'] not in duplicate_users, raw_depression_data))
purified_anxiety_data = list(filter(lambda x: x['author'] not in duplicate_users, raw_anxiety_data))
purified_ocd_data = list(filter(lambda x: x['author'] not in duplicate_users, raw_ocd_data))
purified_bipolar_data = list(filter(lambda x: x['author'] not in duplicate_users, raw_bipolar_data))
purified_bpd_data = list(filter(lambda x: x['author'] not in duplicate_users, raw_bpd_data))
purified_control_data = list(filter(lambda x: x['author'] not in duplicate_users, raw_control_data))

with open('purified_depression_data.json', 'w') as f:
    json.dump(purified_depression_data, f)
    
with open('purified_anxiety_data.json', 'w') as f:
    json.dump(purified_anxiety_data, f)

with open('purified_ocd_data.json', 'w') as f:
    json.dump(purified_ocd_data, f)

with open('purified_bipolar_data.json', 'w') as f:
    json.dump(purified_bipolar_data, f)

with open('purified_bpd_data.json', 'w') as f:
    json.dump(purified_bpd_data, f)

with open('purified_control_data.json', 'w') as f:
    json.dump(purified_control_data, f)






