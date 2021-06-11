"""
MongoDB : get entities name -> add token -> add/update user dict in local
"""
import pandas as pd
import hgtk, os, re
from tqdm.notebook import tqdm
import pymongo
from secret import env

mongodb_url = env.MONGODB_URL.format(username=env.USERNAME, password=env.PASSWORD)
client = pymongo.MongoClient(mongodb_url)
db=client[env.DBNAME]
collection = db[env.COLLECTION_ENTITIES]
tag_dict = {}

tag_dict['NNG-일반명사-T'] = (1780, 3533)
tag_dict['NNG-일반명사-F'] = (1780, 3534)

tag_dict['NNP-고유명사-T'] = (1786, 3545)
tag_dict['NNP-고유명사-F'] = (1786, 3546)

tag_dict['NNP-법률용어-T'] = (1786, 3545)
tag_dict['NNP-법률용어-F'] = (1786, 3546)

tag_dict['XR-어근-T'] = (2423, 3584)
tag_dict['XR-어근-F'] = (2423, 3583)


def add_token(token, tag_type):
    user_dict = []
    global tag_dict
    if tag_type == '일반명사':
        pos = 'NNG'
    elif tag_type == '고유명사':
        pos = 'NNP'
    elif tag_type == '어근':
        pos = 'XR'
    else:
        raise ArgError('token, tag 의 형식을 지켜주세요')
    if hgtk.checker.is_hangul(token) == False:
        batchim = 'F'
    else:    
        batchim = 'F' if hgtk.letter.decompose(token[-1]) == '' else 'T' 
    tag_key = f'{pos}-{tag_type}-{batchim}'
    ltag, rtag = tag_dict[tag_key]
    tlen = len(token)
    if tlen == 1:
        cost = 3000
    elif tlen == 2:
        cost = 2500
    elif tlen == 3:
        cost = 2000
    elif tlen == 4:
        cost = 1500
    elif tlen == 5:
        cost = 1000
    else:
        cost = 500
    result = [token, ltag, rtag, cost, pos, tag_type, batchim, '*','*', '*', '*', '*']
    
    return result

def update_dict():
    global entries
    user_dict = []
    entry_list = entries
    for token, tag_type in entry_list:
        user_dict.append(add_token(token, tag_type))
    pd.DataFrame(user_dict).to_csv('../utils/mecab-ko-dic-2.1.1-20180720/user_dic.csv', index=None, header=None)
    command1 = "../utils/mecab-ko-dic-2.1.1-20180720/tools/add-userdic.sh"
    command2 = "sudo make -S -C ../utils/mecab-ko-dic-2.1.1-20180720/ install" #can be any command but don't forget -S as it enables input from stdin
    assert os.system(command1) == 0, "Fail to add user_dict"
    assert os.system(command2) == 0, "Cannot make install command" # 맥에서 잘 안됨>ubuntu에서는 가능 terminal 에서 진행
    
    print('Update done')
    return user_dict

if __name__ == '__main__':
    entries=[]
    for i in list(collection.find({})):
        token, tag = i['token'],i['tag']
        entries.append([token,tag])

    update_dict()





