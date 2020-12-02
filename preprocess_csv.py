import pandas as pd
from ekphrasis.classes.preprocessor import TextPreProcessor 
from ekphrasis.classes.tokenizer import SocialTokenizer
from ekphrasis.dicts.emoticons import emoticons
import re
import sys

# import initial csv and then overwrite the tweets contents / using columns tweet, keyword, and location
finalDataframe = pd.read_csv(sys.argv[1], usecols=[4, 5, 6])

# get just the tweets to process
tweets = pd.read_csv(sys.argv[1], usecols=[4])
df = tweets.values.tolist()
removalCount = 0
flat = []
for i in df:
    for j in i:
        if pd.isnull(j) != True:
            flat.append(j)
        else:
            # dropping tweet rows that have a tweet value of NaN
            finalDataframe = finalDataframe.drop([removalCount])
        removalCount = removalCount + 1

# create preprocessor to use        
text_processor = TextPreProcessor(
    # terms that will be normalized
    normalize=['url', 'email', 'percent', 'money', 'phone', 'user',
        'time', 'url', 'date', 'number'],
    omit=['url', 'email', 'percent', 'money', 'phone', 'user',
        'time', 'url', 'date', 'number'],

    fix_html=True,  # fix HTML tokens
    
    # corpus from which the word statistics are going to be used 
    # for word segmentation 
    segmenter="twitter", 
    
    # corpus from which the word statistics are going to be used 
    # for spell correction
    corrector="twitter", 
    
    unpack_hashtags=True,  # perform word segmentation on hashtags
    unpack_contractions=True,  # Unpack contractions (can't -> can not)
    spell_correct_elong=True,  # spell correction is set to True to help topic model 
    all_caps_tag="every",
    remove_tags=True,
    
    # select a tokenizer. You can use SocialTokenizer, or pass your own
    # the tokenizer, should take as input a string and return a list of tokens
    tokenizer=SocialTokenizer(lowercase=True).tokenize
    )

X = [" ".join(text_processor.pre_process_doc(x)) for x in flat]
                  
finalDataframe['tweet'] = X

stateList = ['AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL','GA',
             'HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
             'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ',
             'NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
             'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

# save dataframe to new csv for each state
for state in stateList:
    finalDataframe[finalDataframe['location']==state].to_csv(state+'.csv', index=False, header=False, columns=['tweet','keyword'])

