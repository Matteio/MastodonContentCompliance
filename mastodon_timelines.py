# Libraries
import json
import logging
import random
import sys
import threading
from datetime import datetime, timezone
from time import time, sleep

import redis
import requests
from bson import json_util
from pymongo import MongoClient
from rich import print

from bs4 import BeautifulSoup

# Logger setup
logger = logging.getLogger('root')
logging.basicConfig(filename='cascading_mastodon.log', filemode='a', format='%(asctime)s | %(filename)s:%(lineno)s | %(funcName)20s() | %(message)s')


def get_mongo_connection():
    """
    Get connection to the MongoDB collection
    """
    print(f"[{datetime.now()} | connect_to_mongo][italic] Connecting to MongoDB..[/italic]")

    try:
        client = MongoClient()
        print(f"[{datetime.now()} | connect_to_mongo][italic green] Connected to MongoDB successfully![/italic green]")

        print(f"[{datetime.now()} | connect_to_mongo][italic] Obtaining database..[/italic]")
        database = client['cascading']
        print(f"[{datetime.now()} | connect_to_mongo][italic green] Database obtained successfully![/italic green]")
        print('-' * 30)
        return database

    except Exception as e:
        print(f"[{datetime.now()} | connect_to_mongo][italic red] Error {e} on line {sys.exc_info()[-1].tb_lineno}[/italic red]")
        print('-' * 30)
        logging.error("Exception occurred while connecting to MongoDB", exc_info=True)
        return None

def get_unexplored_instances(db):
    """
    Get the list of Mastodon instances to be still explored
    db: the MongoDB reference where users can be found
    """
    try:
        print(f"[{datetime.now()} | get_unexplored_instances][italic] Retrieving unexplored instances from db..[/italic]")
        instances_collection = db['instances'].find({'processed': False})
        print(f"[{datetime.now()} | get_unexplored_instances][italic green] Unexplored instances successfully retrieved from db![/italic green]")
        print('-' * 30)
        return [instance for instance in instances_collection]
    except Exception as e:
        print(f"[{datetime.now()} | get_unexplored_instances][italic red] Error {e} on line {sys.exc_info()[-1].tb_lineno}.[/italic red]")
        logging.error("Exception occurred while obtaining unexplored instances", exc_info=True)

def get_partitions(db, n: int):
    """
    Partition unexplored instances according to the number of available threads
    db: the MongoDB reference where instances can be found
    n : number of available threads to be used in the crawling
    """
    # Let's create N partition (N crawlers)
    partitions = [[] for _ in range(n)]
    print(f"[{datetime.now()} | get_partitions][italic green] {len(partitions)} empty partition(s) created[/italic green]")

    # Let's get data to send into partition
    instances_list = get_unexplored_instances(db)

    # Just more randomness for the crawlers
    # random.shuffle(instances_list)

    print(f"[{datetime.now()} | get_partitions][italic green] {len(instances_list)} unexplored instance(s) retrieved[/italic green]")

    # Let's split data into users
    for instance in instances_list:
        partitions[hash(instance['instance']) % n].append(json_util.dumps(instance))

    # Partitioning completed
    counter = 0
    for index, partition in enumerate(partitions):
        counter += len(partition)
        print(f"[{datetime.now()} | get_partitions][italic] Partition {index + 1} contains {len(partition)} element(s)[/italic]")

    print(f"[{datetime.now()} | get_partitions][italic] Original items to be distributed = {len(instances_list)}[/italic]")
    print(f"[{datetime.now()} | get_partitions][italic] Distributed items = {counter}[/italic]")
    print('-' * 30)

    return partitions

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


def get_timeline(id, db, n, max_iter):
    """
    Main method for the extraction of the timelines
    id: identifier of the current thread
    db: reference to the MongoDB collection
    n: number of currently crawling threads
    """
    
    #known_apps = ['Moa', 'poster', 'Mastodon Twitter Crossposter']

    try:
        # Get redis connection
        r = redis.StrictRedis()

        # Start operating (iterate while no data remains)
        condition = True
        while condition:
            # Get next item from the proper queue
            # If queue will be empty for 60s, it will stop working!
            data = r.blpop('queue' + str(id), timeout=60)

            # If 60s pass, data will be None
            if data is None:
                condition = False

            # We have data from Redis!   
            else:
                try:
                    # Obtain user info
                    instance_metadata = json_util.loads(data[1])
                    mongo_id = instance_metadata['_id']
                    instance_name = instance_metadata['instance']
                    last_seen = int(instance_metadata['last_tl_id'])
                    print(f"[{datetime.now()} | Crawler #{id:<2}] [bold magenta]Visiting {instance_name:<19}[/bold magenta] (last_seen_id = {last_seen})")

                    # URLs and headers
                    header_network = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0'}
                    params = {'limit': '40', 'local': 'true'}
                    
                    # Resume if already explored partially
                    if last_seen != -1:
                        params['max_id'] = last_seen

                    url_timeline = f"https://{instance_name}/api/v1/timelines/public"

                    ### Statuses ###
                    current_iter = 0
                    total_statuses = 0
                    cross_posted_statuses = 0
                    reblog_count = 0
                    reblogged_statuses = 0

                    while current_iter < max_iter and condition:
                        sleep(1) # Being polite is always fun! :)

                        # Let's read the timeline!
                        page_response = requests.get(url_timeline, headers=header_network, params=params, timeout=10)

                        # Reset last_seen, no more needed
                        if 'max_id' in params:
                            del params['max_id']

                        if page_response.status_code == requests.codes.ok:
                            page_response_content = page_response.json()

                            if len(page_response_content) > 0:
                                # Here we store all data to be pushed about reblogs
                                to_be_pushed = []

                                total_statuses += len(page_response_content)
                                for status in page_response_content:
                                    last_seen = status['id']
                                    assert last_seen == status['id']

                                    # Excluding reblogs
                                    if 'reblog' in status and status['reblog'] is not None and status['reblog'] == 'true':
                                        continue

                                    # Asserting local status only
                                    if 'reblog' in status and status['reblog'] is not None and status['reblog'] == 'false':
                                        assert '@' not in status['account']['acct'], "Not a local user?"

                                    # All assertions are ok, we fix the 'local' user, i.e., couple it with its instance
                                    status['account']['acct'] = status['account']['acct'] + '@' + instance_name

                                    # Extracting content
                                    content = status['content']
                                    text = BeautifulSoup(content, 'lxml').get_text()
                                    
                                    crosspost = False

                                    # This should never occur, but we check it anyway
                                    if status['reblog'] == 'true':
                                        print(status)
                                        break

                                    # Checking if it is crossposted
                                    # 1. We match some app names
                                    # print(status)
                                    #if 'application' in status and status['application'] is not None:
                                    #    if 'name' in status['application'] and status['application']['name'] is not None:
                                    #        app_name = status['application']['name'].lower()
                                    #        for name in known_apps:
                                    #            if name.lower() in app_name:
                                    #                crosspost = True
                                    #                break
                                    
                                    # # 2. We match some signals
                                    # if not crosspost and 'RT @' in content or 'crosspost' in content.lower() or '🐦🔗' in content:
                                    #     crosspost = True

                                    # If it is cross-posted, push it!
                                    #if crosspost:
                                    #    cross_posted_statuses += 1
                                    #    rebloggers = []
                            
                                        # Checking if it has re-blogs too
                                    #    if status['reblogs_count'] > 0:
                                    #        # Preparing reblogs url and params
                                    #        url_reblogs = f"https://{instance_name}/api/v1/statuses/{last_seen}/reblogged_by"
                                    #        params_reblogs = {'limit': '80'}

                                            # Getting reblogging users
                                    #        rebloggers = get_reblogs(url=url_reblogs, header=header_network, params=params_reblogs, instance=instance_name, id=id, n=n)

                                            # We need to get reblogs!
                                    #        reblog_count += len(rebloggers)
                                    #        reblogged_statuses += 1
                                        
                                        # In any case, even without reblogs, we push the cross-posted status
                                        # Preparing data for db insertion
                                    record = {
                                            'x_id' : status['id'],
                                            'x_instance' : instance_name,
                                            'x_ts' : status['created_at'],
                                            'x_text' : text,
                                            'x_replies' : status['replies_count'],
                                            'x_reblogs' : status['reblogs_count'],
                                            'x_favourite' : status['favourites_count'],
                                            'x_app_name' : status['application']['name'],
                                            'x_user_handle' : status['account']['acct'],
                                            'x_user_ts' : status['account']['created_at'],
                                            'x_user_followers' : status['account']['followers_count'],
                                            'x_user_following' : status['account']['following_count'],
                                            'x_user_statuses' : status['account']['statuses_count'],
                                            'x_user_last_status' : status['account']['last_status_at']
                                        }

                                    to_be_pushed.append(record)

                                # We iterated over all statuses, now we can push them
                                if len(to_be_pushed) > 0:
                                    db['xposts'].insert_many(to_be_pushed)                

                                print(f"[{datetime.now()} | Crawler #{id:<2}] [bold]{instance_name:<19}[/bold] | Statuse(s) = {total_statuses:>5} | Crosspost(s) = {cross_posted_statuses:>8}"
                                        f" | Reblogged = {reblogged_statuses:>5} | Reblog(s) = {reblog_count:>5} | last_TL_ID = {last_seen:<18}")
                            
                                # We can update the db with the lastest timeline id seen so far
                                db['instances'].update_one({'_id':mongo_id}, {'$set': {'last_tl_id':last_seen, 'total_statuses':total_statuses}})

                                # Checking if there is more data here
                                if 'X-RateLimit-Remaining' in page_response.headers:
                                    remaining_queries = page_response.headers['X-RateLimit-Remaining']
                                    rate_limit_reset = page_response.headers['X-RateLimit-Reset']
                                    check_rate_limits(remaining_queries=remaining_queries, rate_limit_reset=rate_limit_reset, instance=instance_name, id=id)

                                if 'next' in page_response.links:
                                    condition = True
                                    current_iter += 1
                                    url_timeline = page_response.links['next']['url']
                                else:
                                    condition = False

                                    # We completed the timeline of the instance, we can mark it as processed
                                    db['instances'].update_one({'_id':mongo_id}, {'$set': {'last_tl_id':last_seen, 'processed':'true'}})
                                    print(f"[{datetime.now()} | Crawler #{id:<2}] [bold green]Timeline completed for {instance_name:<19}![/bold green] | last_TL_ID = {last_seen:<25}")

                            # No content
                            else:
                                condition = False

                        # Status code is not ok?        
                        else:
                            condition = False
                
                except Exception as e:
                    # Update the queue
                    instance_metadata['lst_tl_id'] = last_seen
                    # r.rpush('queue' + str(id), json_util.dumps(instance_metadata))
                    
                    # Update the db
                    db['instances'].update_one({'_id':mongo_id}, {'$set': {'last_tl_id':last_seen}})
                    
                    print(f"[{datetime.now()} | Crawler #{id:>2}][italic red] Error ({e}) "
                            f"on line {sys.exc_info()[-1].tb_lineno}"
                            f" while visiting {instance_name} @lst_seen_id={last_seen}[/italic red]")
                    #print(page_response_content)
                    sleep(1)
                    # We log only this exception because others are expected to occur sometime
                    logging.error(f"Exception occurred while exploring {instance_name}", exc_info=True)
                    continue

    except Exception as e:
        print(f"[{datetime.now()} | Crawler #{id:>2}][italic red] Error ({e}) on line {sys.exc_info()[-1].tb_lineno}"
              f" while expanding social graphs![/italic red]")

def crawl():
    """
    Method for starting and coordinating all crawling threads
    """
    try:
        # Get redis connection
        r = redis.StrictRedis()

        # Empty all the lists
        r.flushall()

        # Get mongodb connection
        db = get_mongo_connection()

        # Preparing crawlers
        n = int(input('Number of crawlers to start = '))
        partitions = get_partitions(db, n)

        niter = int(input('Number of iterations (x40 toots) to perform = '))

        # i-th queue will contain i-th chunk for i-th crawler
        # Producer step
        for i, chunk in enumerate(partitions):
            if len(chunk) > 0:
                # A bit of shuffling to reduce rate limits risk
                random.shuffle(chunk)
                r.rpush('queue' + str(i), *chunk)

        crawlers = []
        for i in range(n):
            # Each crawler knows how many crawlers there are (n)
            crawlers.append(threading.Thread(target=get_timeline, kwargs={'id': i, 'n': n, 'db': db, 'max_iter': niter}))

        start_time = time()
        for crawler in crawlers:
            crawler.start()

        for crawler in crawlers:
            crawler.join()

        print(f"[{datetime.now()} | start_crawling][bold blue] Time elapsed = {time() - start_time} sec.[/bold blue]")
    except Exception as e:
        print(f"[{datetime.now()} | start_crawling][italic red] Error {e} while crawling![/italic red]")
        logging.error("Exception occurred while starting crawling operations!", exc_info=True)


if __name__ == '__main__':
    crawl()
