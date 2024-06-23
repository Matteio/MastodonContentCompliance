import sys
#import pandas as pd
from datetime import datetime
#from pymongo import MongoClient
from time import time, sleep
#import bson
import requests
import json

def get_instances_list(num_instances, criterion):
    # Getting the list of known Mastodon instances to date
    instances_URL = 'https://instances.social/api/1.0/instances/list'
    instances_social_token = 'HoFTMtjXH0c7Srfsm17xduZaBttnINdpz5nCXUoJBPIny8CKRUdRAwMtE5oU5mVplrnBXD2EXl1hJbd9t1isXQC7O5NgTVjQyt38FZhLMGiDSiVOLexCc92SBsqLvhZp'
    instances_social_header = {"Authorization": "Bearer " + instances_social_token}
    include_dead = "false"
    include_down = "true"
    include_closed = "true"
    count = num_instances
    sort_by = f"{criterion}"
    sort_order = "desc"

    # Asking for the list
    try:
        params={'count': count, 'include_dead': include_dead, 
                                        'include_down': include_down, 'include_closed': include_closed,
                                        'count': count, 'sort_by': sort_by, 'sort_order': sort_order}
        print(params)
        response = requests.get(url=instances_URL, headers=instances_social_header, timeout=30, params=params)

        # If response is ok, process data
        if response.status_code == requests.codes.ok:
            response_json = json.loads(response.text)
            instances_list = response_json['instances']

            print("Instances found: ", len(instances_list))
            found_instances = [elem['name'] for elem in instances_list]
            return found_instances

        # Otherwise, notify me!
        else:
            print(
                f"Status code {response.status_code} while asking for the instances list!")
    except Exception as e:
        print(e)

'''
def init_timeline():
    try:
        print(f"[{datetime.now()} | connect_to_mongo] Connecting to MongoDB..")

        client = MongoClient()
        print(f"[{datetime.now()}] Connected to MongoDB successfully!")

        print(f"[{datetime.now()}] Obtaining database..")
        database = client['cascading']
        print(f"[{datetime.now()}] Database obtained successfully!")
        print('-' * 50)

        # Getting the list of Mastodon instances
        num_instances = int(input(f"[{datetime.now()}] Number of instances to be processed > "))
        criterion = input(f"[{datetime.now()}] Criterion to select instances > ")
        instances = get_instances_list(num_instances, criterion)

        # print(f"[{datetime.now()}] Loading data..\n")
        
        # try:
        #     path = input(f"[{datetime.now()}] Path to instances > ")
        #     with open(path, "r") as _path:
        #         instances = [i.strip() for i in _path.readlines()]
        # except Exception as e:
        #     print(e)

        # print('-' * 50)

        # print(f"[{datetime.now()}] Preparing data..")

        to_be_pushed = []
        for instance in instances:
            to_be_pushed.append(
                {'instance': instance,
                'last_tl_id': bson.Int64(-1),
                'processed': False,
                'total_statuses': 0}
            )

        print(f"[{datetime.now()}] Elements to be pushed: {len(to_be_pushed)}")
        print('-' * 50)
        
        print(f"[{datetime.now()}] Pushing instances..")
        result = database['instances'].insert_many(to_be_pushed)

        sleep(1)
        inserted_docs = database['instances'].count_documents({})
        if inserted_docs == len(to_be_pushed):
            print(f"[{datetime.now()}] Data pushed successfully ({inserted_docs} elements)")
        else:
            print(f"[{datetime.now()}] Error while pushing data: ({inserted_docs} elements pushed)")


    except Exception as e:
        print(f"[{datetime.now()}] Error {e} on line {sys.exc_info()[-1].tb_lineno}")
        print('-' * 50)
'''
if __name__ == '__main__':
    instances = get_instances_list(0, 'name')
    save_dir = 'C:\\Users\\Nicola\\Downloads\\'
    with open(f'{save_dir}instances.json','w') as f:
        json.dump({'instances':instances},fp=f)