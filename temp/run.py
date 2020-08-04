#!/usr/bin/python3 -u
#encoding:UTF-8
import time, os, json, os, sys
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress
from steem.blockchain import Blockchain
from steem.steemd import Steemd
from elasticsearch import Elasticsearch 
import pymysql

env_dist = os.environ

# init block config
print('-------- env params --------')
steemd_url = env_dist.get('STEEMD')
if steemd_url == None or steemd_url == "":
    steemd_url = 'https://api.steem.fans'
print('STEEMD: %s' % steemd_url)

worker_num = env_dist.get('WORKER_NUM')
if worker_num == None or worker_num == "":
    worker_num = 10
print('WORKER_NUM: %s' % (worker_num))
worker_num = int(worker_num)

step = env_dist.get('STEP')
if step == None or step == "":
    step = 100
print('STEP: %s' % (step))
step = int(step)

start_block_num = env_dist.get('START_BLOCK_NUM')
if start_block_num == None or start_block_num == "":
    start_block_num = 1
print('START_BLOCK_NUM: %s' % (start_block_num))
start_block_num = int(start_block_num)

es_url = env_dist.get('ES_URL')
if es_url == None or es_url == "":
    print('Please set ES_URL')
    exit(1)
print('ES_URL: %s' % (es_url))

es_user = env_dist.get('ES_USER')
if es_user == None or es_user == "":
    print('Please set ES_USER')
    exit(1)
print('ES_USER: %s' % (es_user))

es_pass = env_dist.get('ES_PASS')
if es_pass == None or es_pass == "":
    print('Please set ES_PASS')
    exit(1)
print('ES_PASS: %s' % (es_pass))

mysql_host = env_dist.get('MYSQL_HOST')
if mysql_host == None or mysql_host == "":
    mysql_host = '127.0.0.1'
print('MYSQL_HOST: %s' % (mysql_host))

mysql_port = env_dist.get('MYSQL_PORT')
if mysql_port == None or mysql_port == "":
    mysql_port = 3306
print('MYSQL_PORT: %s' % (mysql_port))

mysql_user = env_dist.get('MYSQL_USER')
if mysql_user == None or mysql_user == "":
    mysql_user = 'root'
print('MYSQL_USER: %s' % (mysql_user))

mysql_pass = env_dist.get('MYSQL_PASS')
if mysql_pass == None or mysql_pass == "":
    mysql_pass = 123456
print('MYSQL_PASS: %s' % (mysql_pass))

mysql_db = env_dist.get('MYSQL_DB')
if mysql_db == None or mysql_db == "":
    mysql_db = 'ugly_db'
print('MYSQL_DB: %s' % (mysql_db))

steemd_nodes = [
    steemd_url,
]
s = Steemd(nodes=steemd_nodes)
b = Blockchain(s)

es = Elasticsearch([es_url], http_auth=(es_user, es_pass))
conn = connect_db()

def connect_db():
    # Connect to the database
    try:
        conn = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_pass,
            db=mysql_db,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        if conn.open == True:
            return conn
        else:
            print('mysql connect error')
            exit()
    except:
        print('mysql is not ready.')
        exit()

def process(block_nums):
    try:
        block_from = block_nums[0]
        block_to = block_nums[1]
        with conn.cursor() as cursor:
            cursor.execute('INSERT INTO block_log (start, end, status) VALUES (%i, %i, %i)' % (int(block_from), int(block_to), 0))
        conn.commit()
        block_infos = s.get_blocks(range(block_from, block_to))
        #print(block_infos)
        op_count = 0
        for block_info in block_infos:
            transactions = block_info['transactions']
            for trans in transactions:
                operations = trans['operations']
                for op in operations:
                    if op[0] == 'account_create' or op[0] == 'create_claimed_account' or op[0] == 'account_update' or op[0] == 'account_create_with_delegation':
                        insert_data = {
                            'block_num': block_info['block_num'],
                            'transaction_id': trans['transaction_id'],
                            'op_type': op[0],
                            'op_detail': op[1],
                        }
                        r = es.index(index='op_index', body=insert_data)
                        op_count = op_count + 1
        with conn.cursor() as cursor:
            cursor.execute('UPDATE block_log SET status = 1 WHERE start = %i and end = %i' % (int(block_from), int(block_to)))
        conn.commit()
        return {
            'start': block_from,
            'end': block_to,
            'op_count': op_count,
        }
    except Exception as e:
        print(e)
        return 0

def parse(future):
    res = future.result()
    if res == 0:
        return
    print('Inserted %i operations from %s to %s' % (res['op_count'], res['start'], res['end']))

if __name__ == '__main__':
    with suppress(KeyboardInterrupt):
        #end_block_num = 5000
        end_block_num = b.info()['last_irreversible_block_num']
        print('end block num: %i' % end_block_num)

        p = ProcessPoolExecutor(worker_num)
        keep = True
        #start_block_num = 4950
        start = start_block_num
        while keep:
            if start + step < end_block_num:
                future = p.submit(process, [start, start+step])
                start = start + step
            else:
                future = p.submit(process, [start, end_block_num + 1])
                keep = False
            future.add_done_callback(parse)
        p.shutdown(wait=True)
