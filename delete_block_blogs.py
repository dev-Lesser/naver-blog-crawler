"""
mongodb get block_blog list -> delete data
"""

import pymongo
from secret import env
from tqdm.notebook import tqdm

mongodb_url = env.MONGODB_URL.format(username=env.USERNAME, password=env.PASSWORD)
client = pymongo.MongoClient(mongodb_url)
db=client[env.DBNAME]


if __name__ == '__main__':
    collection = db[env.COLLECTION_BLOCK]
    blogs=list(collection.find({},{'_id':0}))

    collection = db[env.COLLECTION_MAIN]
    for blog in blogs:
        collection.delete_many({'blog_id':blog})
        print('Deleted blog id = {}'.format(blog))