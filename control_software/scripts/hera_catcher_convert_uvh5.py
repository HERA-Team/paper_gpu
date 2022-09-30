#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import logging
import psutil

logger = logging.getLogger(__file__)

REDISHOST = 'redishost'
DATA_DIR = '/data'
RAW_EXT = 'dat'
TEMPLATE = re.compile(r'zen\.(\d+)\.(\d+)\.(sum|diff)\.dat')
RAW_FILE_KEY = 'corr:raw_files'
CONV_FILE_KEY = 'corr:converted_files'
CUR_FILE_KEY = 'corr:current_file'
CPU_AFFINITY = list(range(6))  # the rest are reserved for the catcher

def match_up_filenames(f, cwd=None):
    path, f_in = os.path.split(f)
    if cwd is not None:
        path = os.path.join(cwd, path)
    jd_day, jd_frac, sum_diff = TEMPLATE.match(f_in).groups()
    f_in = os.path.join(path, f_in)
    f_meta = os.path.join(path, f'zen.{jd_day}.{jd_frac}.meta.hdf5')
    f_out = os.path.join(path, f'zen.{jd_day}.{jd_frac}.{sum_diff}.hdf5')
    return (f_in, f_meta, f_out)

def get_cwd_from_redis(r):
    return r.hget('corr', 'catcher_cwd')

def process_next(r, cwd):
    p = psutil.Process()
    p.cpu_affinity(CPU_AFFINITY)
    if r.llen(RAW_FILE_KEY) == 0:
        return
    # once we get a key, we either have to finish it or return it; no dropping
    logger.info(f'Processing {f}')
    f = r.rpop(RAW_FILE_KEY)  # process most recent first (LIFO)
    try:
        f_in, f_meta, f_out = match_up_filenames(f, cwd)
        make_uvh5_file(f_out, f_meta, f_in)
        f_out_rel = os.path.relpath(f_out, cwd)
        r.rpush(CONV_FILE_KEY, f_out_rel)  # document we finished it
        logger.info(f'Finished {f}')
    except Exception as e:
        r.lpush(RAW_FILE_KEY, f)  # otherwise return key at back of queue
        logger.error(f'Failed to finish {f}. Returned to queue.\n' + str(e))
    
if __name__ == '__main__':
    import multiprocessing as mp
    import redis
    import time
    import argparse

    assert psutil.cpu_count() == 12, "if this errors, you're not on hera-sn1"

    r = redis.Redis(REDISHOST)
    cwd = get_cwd_from_redis(r)
    with mp.Pool(len(CPU_AFFINITY)) as pool:
        nworkers = pool._processes
        while True:
            try:
                qlen = r.llen(RAW_FILE_KEY)
                active = len(mp.active_children())
                logger.info(f'Queue length={qlen}, N workers={active}/{nworkers}')
                if qlen > 0 and len(mp.active_children()) < nworkers:
                    pool.apply_async(process_next, (r, cwd))
                else:
                    time.sleep(1)
            except(KeyboardInterrupt):
                pool.close()
                pool.join()
