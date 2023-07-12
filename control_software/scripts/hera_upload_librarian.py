#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import subprocess
import logging
import psutil
from hera_librarian import LibrarianClient

logger = logging.getLogger(__file__)

REDISHOST = 'redishost'
DATA_DIR = '/mnt/sn1'
CONV_FILE_KEY = 'corr:files:converted'
PURG_FILE_KEY = 'corr:files:lib_purgatory'
FAILED_FILE_KEY = 'corr:files:lib_failed'
LIB_FILE_KEY = 'corr:files:uploaded'
JD_KEY = 'corr:files:jds'
CONN_NAME = 'local-rtp'
CPU_AFFINITY = [3, 4, 5, 6]
ADD_DIFF_TO_LIBRARIAN = False

def filter_done(f, thd):
    is_alive = thd.is_alive()
    if not is_alive:
        purgfiles = r.hgetall(PURG_FILE_KEY)
        if f in purgfiles:
            # failure to remove from purgatory indicates failed upload
            # so clean up and add to failed queue
            r.rpush(FAILED_FILE_KEY, f)
            r.hdel(PURG_FILE_KEY, f)
    return is_alive


def process_next(f):
    p = psutil.Process()
    p.cpu_affinity(CPU_AFFINITY)
    print(f'Processing {f}')
    client = LibrarianClient(CONN_NAME)
    query = f'{{"name-matches": "{os.path.split(f)[-1]}"}}'
    instances = client.search_instances(query)  # takes a long time
    if len(instances['results']) == 0:
        # this file is not in the librarian, so add it
        full_path = os.path.join(DATA_DIR, f)
        print(f'Uploading {f}')
        client.upload_file(full_path, f, 'infer', rec_info={})
    else:
        print(f'Librarian already has {f}')
    r.rpush(LIB_FILE_KEY, f)  # document we finished it
    r.hdel(PURG_FILE_KEY, f)
    print(f'Finished {f}')

if __name__ == '__main__':
    import multiprocessing as mp
    import redis
    import time

    p = psutil.Process()
    p.cpu_affinity(CPU_AFFINITY)

    logging.basicConfig(level=logging.DEBUG)

    r = redis.Redis(REDISHOST, decode_responses=True)
    qlen = r.llen(CONV_FILE_KEY)
    nworkers = 3 * len(CPU_AFFINITY)
    print(f'Starting librarian upload.')
    children = {}
    try:
        while True:
            qlen = r.llen(CONV_FILE_KEY)
            children = {f: thd for f, thd in children.items()
            if filter_done(f, thd)}:
                print(f'Queue length={qlen}, N workers={len(children)}/{nworkers}')
            if qlen > 0 and len(children) < nworkers:
                # once we get a key, we commit to finish it or return it; no dropping
                f = r.rpop(CONV_FILE_KEY)  # process most recent first (LIFO)
                if (not ADD_DIFF_TO_LIBRARIAN) and ("diff" in f):
                    continue
                r.hset(PURG_FILE_KEY, f, 0)
                print(f'Starting worker on {f}')
                thd = mp.Process(target=process_next, args=(f,))
                thd.start()
                children[f] = thd
            elif qlen == 0 and len(children) == 0:
                # caught up and queue is empty, so check if we finished any days
                jds = r.hgetall(JD_KEY)
                for jd, val in jds.items():
                    if int(val) == 1:
                        r.hset(JD_KEY, jd, int(val) + 1)
                time.sleep(10)
            else:
                # we are still working and should wait for jobs to complete
                time.sleep(2)
    except Exception as e:
        print(f'Closing down {len(children)} threads')
        for f, thd in children.items():
            thd.terminate()
        for thd in children.values():
            thd.join()
    finally:
        print('Cleanup')
        purgfiles = r.hgetall(PURG_FILE_KEY)
        for f in purgfiles:
            print(f'Returning {f}')
            r.rpush(CONV_FILE_KEY, f)
            r.hdel(PURG_FILE_KEY, f)
