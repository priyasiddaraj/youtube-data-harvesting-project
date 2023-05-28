from pymongo import MongoClient
def mongo_connect():
    client= MongoClient('localhost',27017)
    db=client.youtube
    col = db.Channels
    return col
def insert_data_into_db(YT_Data):
    coll=mongo_connect()
    coll.insert_one(YT_Data)
    return 'success'

def list_channel():
    coll=mongo_connect()
    list_channels = [i['Channel_Details']['channelName'] for i in coll.find({})]
    return list_channels

def channel_details_mogo(channelName):
    coll=mongo_connect()
    chann_List=[]
    for i in coll.find({}):
        if i['Channel_Details']['channelName'] ==channelName:
            chann_List.append(i)
    return chann_List[0]

def Channel_id_uniq(cid):
    coll = mongo_connect()
    for i in coll.find({}):
        if i['Channel_Details']['channel_id'] == cid:
            return True
        else:
            return False


