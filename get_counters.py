import requests
from bs4 import BeautifulSoup
import json
import os
import threading
import logging

n_threads = 40
def get_instances(file):
    with open(file, 'r') as f:
        return [json.loads(line) for line in f.readlines()]
    
results = [0]*n_threads
    
def crawl_posts_counters(instances, thread_id):
    counters = {'instances':[]}
    for instance in instances:
        instance_name = instance['instance']
        post_months_url = f"https://{instance_name}/api/v1/instance/activity"
        try:
            response = requests.get(post_months_url, timeout=10)
        except Exception:
            continue
        try:
            if response.status_code == requests.codes.ok:
                months_post = response.json()
                counter = 0
                for obj in months_post:
                    counter+=int(obj['statuses'])
                counters['instances'].append( {'instance': instance_name, 'counter':counter})
        except Exception:
            continue

    #with open(f'./jsons/counters{thread_id}.json','w') as f:
    #    json.dump(counters,f,indent=4)
    results[thread_id] = counters
    print(f'thread{thread_id} finished')  

    

if __name__ == '__main__':
    mastodon = get_instances('./instances.jsonl')
    split = []
    size = len(mastodon)//n_threads
    for i in range(0,len(mastodon),size):
        split.append(mastodon[i:i+size])

    crawlers = []
    for i in range(n_threads):
        crawlers.append(threading.Thread(target=crawl_posts_counters, kwargs={'instances':split[i], 'thread_id':i}))
    print(len(crawlers))
    for crawler in crawlers:
        crawler.start()

    for crawler in crawlers:
        crawler.join()

    print(results)
    l = []
    for counters in results:
        for elem in counters['instances']:
            l.append(elem)
    with open('./counters.json','w') as f:
        json.dump({'instances':l},f,indent=4)