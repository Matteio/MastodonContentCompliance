import requests
from bs4 import BeautifulSoup
import json
import os
import threading
import datetime
from datetime import timedelta, timezone, datetime
from time import sleep
import sys


n_instances = 1000
n_t = 40
total_instances = [0]*n_t

def is_in_3months(t):
    
    # Time provided by Mastodon is in Zulu time
    refresh_time = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
    current_time = datetime.now(timezone.utc)

    # Remove timezone info for comparison
    current_time = current_time.replace(tzinfo=None)
    refresh_time = refresh_time.replace(tzinfo=None)

    # Check if the refresh time is more than 90 days ago
    return current_time - refresh_time <= timedelta(days=90)


def get_unprocessed_instances(file):
    with open(file, 'r') as f:
        a = [json.loads(line) for line in f.readlines()]
        return [elem for elem in a if not elem['processed']]
    
def get_waiting_time(t):
    """
    Gets the waiting time w.r.t. the current rate limit reset time
    t: current reset time
    """
    # Time provided by Mastodon is Zulu time
    refresh_time = datetime.strptime(t, "%Y-%m-%dT%H:%M:%S.%fZ")
    current_time = datetime.now(timezone.utc)

    # Current time has timezone info, unlike refresh_time
    current_time = current_time.replace(tzinfo=None)

    if refresh_time > current_time:
        waiting_time = refresh_time - current_time
        return waiting_time.seconds
    else:
        return 0

def check_rate_limits(remaining_queries, rate_limit_reset, instance, id, n=5):
    """
    Checking if rate limit is approaching
    remaining_queries: number of queries to the rate limit
    rate_limit_reset: reset time for the next rate limit
    instance: server where the rate limit applies
    id: current thread
    n: tolerance w.r.t. the number of remaining queries (which corresponds to the number of threads or a fixed number)
    """
    # No try catch here, if it fails, we should consider it as unprocessed and process it later
    if int(remaining_queries) <= int(n):  # a bit of trade-off, to avoid getting banned
        waiting_time = get_waiting_time(rate_limit_reset)
        if waiting_time > 0:
            print(f"[{datetime.now()} | Crawler #{id:>2}][bold magenta] Rate limit reached for {instance} waiting for {waiting_time} sec.[/bold magenta]")
            sleep(waiting_time)
            print(f"[{datetime.now()} | Crawler #{id:>2}][bold magenta] Rate limit ok for {instance}![/bold magenta]")

    
def get_unprocessed_split(n, file):
    unp = get_unprocessed_instances(file)
    size = n_instances//n
    it = iter(unp)
    i=0
    while i < n:
        item = []
        while len(item) < size:
            try:
                instance = next(it)
                instance_name = instance['instance']
                response = requests.get(f'https://{instance_name}/api/v1/timelines/public',params={'limit':'20'}, timeout=5)
                if response.status_code == requests.codes.ok:
                    item.append(instance)
            except Exception:
                continue
        print(f'creating json number {i+1}')
        with open(f'./jsons/instances{i}.json', 'w') as f:
            json.dump(item, f, indent=4)
        i+=1


def crawl_instance(instance_dict, iterations, thread_id):
    instance_name = instance_dict['instance']
    last_tl_id = instance_dict['last_tl_id']
    instance_rules_url = f'https://{instance_name}/api/v1/instance/rules'
    try:
        rules = requests.get(instance_rules_url).json()
        instance = {
            'name':instance_name,
            'rules': rules,
            'records':[]
        }
            
        header_network = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}
        params = {'limit': '40', 'local': 'true'}
        url_timeline = f"https://{instance_name}/api/v1/timelines/public"

        if last_tl_id != -1:
            params['max_id'] = last_tl_id
        for _ in range(iterations):
            page_response = requests.get(url_timeline, headers=header_network, params=params, timeout=10)
            if page_response.status_code == requests.codes.ok:
                response_json = page_response.json()
                for status in response_json:
                    date = status['created_at']
                    if(is_in_3months(date)):
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
                    else:
                        break
            if 'X-RateLimit-Remaining' in page_response.headers:
                remaining_queries = page_response.headers['X-RateLimit-Remaining']
                rate_limit_reset = page_response.headers['X-RateLimit-Reset']
                check_rate_limits(remaining_queries=remaining_queries, rate_limit_reset=rate_limit_reset, instance=instance_name, id=thread_id)
    except Exception:
        print(f'l\' istanza {instance_name} non risponde')
        return None
    with open(f'./results/{instance_name}.json','w') as f:
        json.dump(instance,f)
    return instance


def thread_execution(thread_id, iterations, already_c):
    my_instances = json.load(open(f'./jsons/instances{int(thread_id)}.json','r'))
    crawled = []
    for instance in my_instances:
        instance_name = instance['instance']
        if instance_name not in already_c:
            crawled.append(crawl_instance(instance,int(iterations), int(thread_id)))
            print(f'thread[{int(thread_id)}] crawled instance: {instance_name}')
        else:
            print(f'instance {instance_name} already crawled')
    total_instances[int(thread_id)] = crawled
    

def run_threads( start, iterations):
    files = os.listdir('./jsons')
    already_c = os.listdir('./results')
    threads = []
    for thread_id in range(start, len(files),2):
        threads.append(threading.Thread(target=thread_execution, kwargs={'thread_id':thread_id, 'iterations':iterations, 'already_c':already_c}))
    
    print('starting threads')
    for thread in threads:
        thread.start()
    
    print('joining threads')
    for thread in threads:
        thread.join()

    print('threads finished')
    flattened = []
    for l in total_instances:
        for item in l:
            flattened.append(item)
    with open(f'./crawling{start}.json', 'w') as f:
        json.dump(flattened, f, indent=4)
    





if __name__ == '__main__':
    start = sys.argv[1]
    it = sys.argv[2]
    print('starting crawling...')
    run_threads(int(start), int(it))
    print('end crawling...')
