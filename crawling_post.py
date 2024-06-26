import requests
from bs4 import BeautifulSoup
import json
import os
import threading

def get_unprocessed_instances(file):
    with open(file, 'r') as f:
        a = [json.loads(line) for line in f.readlines()]
        return [elem for elem in a if not elem['processed']]
    

def crawl_instance(instance_dict, file_name, limit):
    instance_name = instance_dict['instance']
    last_tl_id = instance_dict['last_tl_id']
    instance_rules_url = f'https://{instance_name}/api/v1/instance/rules'
    rules = requests.get(instance_rules_url).json()
    instance = {
        'name':instance_name,
        'rules': rules,
        'records':[]
    }
        
    header_network = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}
    params = {'limit': str(limit), 'local': 'true'}
    url_timeline = f"https://{instance_name}/api/v1/timelines/public"

    if last_tl_id != -1:
        params['max_id'] = last_tl_id
    page_response = requests.get(url_timeline, headers=header_network, params=params, timeout=10)
    response_json = page_response.json()
    for status in response_json:
        content = status['content']
        text = BeautifulSoup(content).get_text()
        record = {
            'id': status['id'],
            'user_id': status['account']['id'],
            'user_posts_count': status['account']['statuses_count'],
            'text':text,
            'tags': status['tags'],
            'language': status['language'],
            'favourites': status['favourites_count']
        }
        instance['records'].append(record)

    if os.path.isfile(file_name):
        jsnobj = json.load(open(file_name,'r'))
    else:
        with open(file_name, 'w') as f:
            jsn = {'instances':[]}
            json.dump(jsn,f,indent=4)
            jsnobj = json.load(open(file_name,'r'))
    
    jsnobj['instances'].append(instance)
    with open(file_name, 'w') as f:
        json.dump(jsnobj,f, indent=4)




