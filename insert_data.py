"""
Mongodb data -> extract keywords -> insert data
"""
import requests
import json
import pandas as pd
import pymongo
import MeCab
import re
from tqdm import tqdm
from collections import Counter
import datetime
from secret import env

mongodb_url = env.MONGODB_URL.format(username=env.USERNAME, password=env.PASSWORD)
client = pymongo.MongoClient(mongodb_url)
db=client[env.DBNAME]
mecab = MeCab.Tagger()
one_list = list('젤펄물향색립손짱굳양톤물티밤코빛붓굿돈팩맥맛감웜집샵끈선속밖겉봄반광팁샷솔볼폼딥입펜발템갑값'
                '똥룩꿀떡존몸핏땀핫옆꽃액칠금쿨차밥병솜망폰링통옷목팔숱배살털컬홈짝뷰')
column =['keyword','keyword_num','bigram','bigram_num','trigram','trigram_num',]

def parse_text(text):
    bucket = []
    start = 0
    pattern = re.compile(".*\t[A-Z]+") 
    for token, pos in  [tuple(pattern.match(token).group(0).split("\t")) for token in mecab.parse(text).splitlines()[:-1]]:
        if start == 0:
            lspace = False
        elif text[start] == ' ':
            lspace = True
        else:
            lspace = False
        start = text.find(token, start)
        end = start + len(token)
        bucket.append((token, pos, lspace, start, end))
        start = end
    return bucket
def apply_noun_ext(df): 
    text = df['contents']
    clean_text = re.sub('\[선물\]|\[추천\]', '', text)
    result = noun_ext(parse_text(clean_text))
    return result

def check_noun(cand):
    if len(re.findall('[^가-힣a-zA-Z0-9+]', cand)) > 0:
        return False
    else:
        return True
def noun_ext(parsed_text):
    global stopwords
    global one_list
    noun_bucket = []
    bi_bucket = []
    tri_bucket = []
    last_last_add = None
    last_add = None
    last_lspace = None
    for ichunk in parsed_text:
        token = ichunk[0]
        pos = ichunk[1]
        lspace = ichunk[2]
        add = False
        if pos == 'NNG': 
            if check_noun(token) == True: 
                if token not in stopwords:
                    if len(token) > 1: 
                        add = True
                    elif token in one_list:
                        add = True
        if pos == 'NNP': 
            if token not in stopwords:
                if len(token) > 1:
                    add = True
                elif token in one_list:
                    add = True
                    
        if add == True:
            if (last_add == True) and (lspace == False) and (len(noun_bucket) > 0):
                bi_bucket.append('|'.join([noun_bucket[-1], token]))
            if (last_add == True) and (last_last_add == True) and (lspace == False) and \
                                        (last_lspace == False) and (len(noun_bucket) > 1):
                tri_bucket.append('|'.join([noun_bucket[-2], noun_bucket[-1], token]))
            noun_bucket.append(token)
        last_lspace = lspace
        last_last_add = last_add
        last_add = add
    return pd.Series((noun_bucket, bi_bucket, tri_bucket))

def get_stopwords(collection):
    stopwords = []
    for i in list(collection.find({},{'_id':0,'stop':1})):
        stopwords.append(i['stop'])
    return stopwords

if __name__ == '__main__':
    collection = db[env.COLLECTION_THEME]
    theme_list = list(collection.find())
    
    now = datetime.datetime.now()
    before = now - datetime.timedelta(days=7)

    
    collection = db[env.COLLECTION_STOPWORDS]
    stopwords = get_stopwords(collection)
    
    for theme in tqdm(theme_list):
        collection = db[env.COLLECTION_MAIN]
        results = list(collection.find({'theme':theme['theme'],'add_date':{'$gte':before,'$lte':now}}))
        df = pd.DataFrame(results)
        df=df[df.columns[1:]]    
        df[['keywords','bigrams','trigrams']] = df.fillna('').apply(apply_noun_ext, axis=1)
        c = Counter()
        c_b = Counter()
        c_t = Counter()

        for i,row in df.iterrows():
            keywords = row['keywords']
            bigrams = row['bigrams']
            trigrams = row['trigrams']
            c.update(keywords)
            c_b.update(bigrams)
            c_t.update(trigrams)
        keyword = [{'word':i[0],'num':i[1]} for i in c.most_common(100)]
        bigram =[{'word':i[0],'num':i[1]} for i in c_b.most_common(100)]
        trigram =[{'word':i[0],'num':i[1]} for i in c_t.most_common(100)]
        now_date = datetime.datetime.now()
        result = {
            'theme':theme['theme'],
            'data':{
                'keyword': keyword,
                'bigram': bigram,
                'trigram': trigram,
            },
            'analysis_date': now_date
        }
        collection = db[env.COLLECTION_ANALYSIS]
        collection.insert_one(result)
        print('Finished insert analysis data\t theme %s' %(theme['theme']))






