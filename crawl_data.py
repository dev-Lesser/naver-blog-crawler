"""
MongoDB : get block list, theme list -> search naver blog keyword: "[theme] 선물 추천" 

        -> parse text data in each blog -> insert data
"""
import requests
from urllib import parse
from lxml import html
import json
import math
import pymongo
import datetime
from secret import env
from tqdm.notebook import tqdm


mongodb_url = env.MONGODB_URL.format(username=env.USERNAME, password=env.PASSWORD)
client = pymongo.MongoClient(mongodb_url)
db=client[env.DBNAME]

now = datetime.datetime.now()
before = now - datetime.timedelta(days=7)

def get_total_count(search_keyword,page_number,startDate,endDate):
    url = 'https://section.blog.naver.com/ajax/SearchList.naver?countPerPage=7&currentPage={page}&endDate={end_date}&keyword={keyword}&orderBy=recentdate&startDate={start_date}&type=post'            .format(keyword=search_keyword, page=page_number, start_date=startDate, end_date=endDate)
    # 7개씩 블로그를 가져옴
    query = {
        'keyword':search_keyword
    }
    encoded_keyword = parse.urlencode(query, encoding='UTF-8', doseq=True)
    headers={
        'referer':'https://section.blog.naver.com/Search/Post.naver?pageNo=1&rangeType=ALL&orderBy=recentdate&{}'.format(page_number,encoded_keyword)
    }
    res = requests.get(url,headers=headers)
    result = json.loads(res.text.strip()[6:]) # 앞쪽에  """ )]}', """ 의 6개의 의미 없는 문자가 있음
    total_count = math.ceil(result['result']['totalCount']/7)
    return total_count 

def get_block_blogs(db):
    
    collection = db[env.COLLECTION_BLOCK]
    block_blogs = []
    for i in list(collection.find()):
        block_blogs.append(i['blog'])
    return block_blogs

def get_theme(db):
    collection = db[env.COLLECTION_THEME]
    theme_list = list(collection.find())
    return theme_list


if __name__ == '__main__':
    # GET 크롤 하지 않을 블로그 id 
    block_blogs = get_block_blogs(db)
    
    # GET 검색할 테마( 가져오기 :  [--] 선물 추천)
    theme_list = get_theme(db)


    collection = db[env.COLLECTION_MAIN]
    for theme in theme_list:
        search_keyword= """"{} 선물" 추천""".format(theme['theme'])
        startDate=before.strftime('%Y-%m-%d')
        endDate=now.strftime('%Y-%m-%d')
        page_number = 1
        loop = get_total_count(search_keyword,page_number,startDate,endDate)

        """
        최신순, 오늘날짜기준 일주일/한달, pagination
        """
        results=[]
        for i in tqdm(range(1,loop+1)):

            url = 'https://section.blog.naver.com/ajax/SearchList.naver?countPerPage=7&currentPage={page}&endDate={end_date}&keyword={keyword}&orderBy=recentdate&startDate={start_date}&type=post'                    .format(keyword=search_keyword, page=i, start_date=startDate, end_date=endDate)
            # 7개씩 블로그를 가져옴
            query = {
                'keyword':search_keyword
            }
            encoded_keyword = parse.urlencode(query, encoding='UTF-8', doseq=True)
            headers={
                'referer':'https://section.blog.naver.com/Search/Post.naver?pageNo=1&rangeType=ALL&orderBy=recentdate&{}'.format(page_number,encoded_keyword)
            }
            res = requests.get(url,headers=headers)
            result = json.loads(res.text.strip()[6:]) # 앞쪽에  """ )]}', """ 의 6개의 의미 없는 문자가 있음 --> 모니터링 필요

            url_data = [(i['postUrl'],i['title'],i['blogId'],i['logNo']) for i in result['result']['searchList']]
            date = [i['addDate'] for i in result['result']['searchList']]


            # 가져온 7개의 url에 대해
            for i in range(len(url_data)):
                (_,_,blogId,logNo)=url_data[i]
                compare = collection.find_one({'blog_id':blogId,'log_no':logNo})
                breakpoint=False

                # 중복된 blod id 와 log no 가 있으면 가장 바깥으로 break 하기 위한 breakpoint 선언
                if compare: 
                    breakpoint=True
                    print('{} \t 중복된 아이디 값( {}, {} ) 이 있어 다음으로 넘어갑니다.'.format(theme['theme'], blogId, logNo))
                    break
                if blogId in block_blogs: continue # block user 가 나왔을때는 데이터를 추가하지 않고 그 다음 url 로 넘어감
                url = 'https://blog.naver.com/PostView.naver?blogId={blogId}&logNo={logNo}&redirect=Dlog&widgetTypeCall=true&from=section&topReferer=https%3A%2F%2Fsection.blog.naver.com%2F&directAccess=false'.format(blogId=blogId, logNo=logNo)
                res = requests.get(url)
                root= html.fromstring(res.text.strip())
                contents = ' '.join([i.strip() for i in root.xpath('//span[contains(@class,"se-fs-")]/text()|//span[contains(@class,"se-fs-")]/*/text()') if i.strip() not in ['','\u200b'] ])

                data = {
                    'theme':theme['theme'],
                    'blog_id': blogId,
                    'log_no': logNo,
                    'url': 'https://blog.naver.com/{}/{}'.format(blogId, logNo),
                    'contents': contents,
                    'add_date':datetime.datetime.fromtimestamp(date[i]/1000)

                }
                
                if contents!='' or not compare:
                    results.append(data)
            if breakpoint:
                break
        if results:
            collection.insert_many(results)
    print('Finished crawling \t',len(results))
