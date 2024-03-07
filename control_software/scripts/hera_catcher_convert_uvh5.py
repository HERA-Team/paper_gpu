#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import os
import psutil
import numpy as np
from paper_gpu.file_conversion import make_uvh5_file
from astropy.time import Time
from hera_mc import mc

DELETE_TAGS = ('delete', 'junk')
REDISHOST = 'redishost'
DATA_DIR = '/data'
TEMPLATE = re.compile(r'zen\.(\d+)\.(\d+)\.(sum|diff)\.dat')
RAW_FILE_KEY = 'corr:files:raw'
PURG_FILE_KEY = 'corr:files:purgatory'
CONV_FILE_KEY = 'corr:files:converted'
FAILED_FILE_KEY = 'corr:files:failed'
JD_KEY = 'corr:files:jds'
#CPU_AFFINITY = list(range(6))  # the rest are reserved for the catcher
CPU_AFFINITY = [2, 3, 4, 5]
MINIMUM_UVH5_RELATIVE_SIZE = 0.1  # if a .uvh5 file is less than 10% the size of the .dat file, don't auto-delete the .dat file

def match_up_filenames(f, cwd=None):
    path, f_in = os.path.split(f)
    if cwd is not None:
        path = os.path.join(cwd, path)
    jd_day, jd_frac, sum_diff = TEMPLATE.match(f_in).groups()
    is_diff = (sum_diff == 'diff')
    f_in = os.path.join(path, f_in)
    f_meta = os.path.join(path, f'zen.{jd_day}.{jd_frac}.meta.hdf5')
    f_out = os.path.join(path, f'zen.{jd_day}.{jd_frac}.{sum_diff}.uvh5')
    return (f_in, f_meta, f_out), is_diff

def get_cwd_from_redis(r, default='/data'):
    cwd = r.hget('corr:catcher', 'cwd')
    if cwd is None:
        return default
    else:
        return cwd

def return_purgatory_files(r):
    purgfiles = r.hgetall(PURG_FILE_KEY)
    for f in purgfiles:
        print(f'Returning {f}')
        r.rpush(RAW_FILE_KEY, f)
        r.hdel(PURG_FILE_KEY, f)
        (f_in, f_meta, f_out), is_diff = match_up_filenames(f, cwd)
        print(f'Remove {f_out}?')
        if os.path.exists(f_out):
            print(f'Removing {f_out}')
            os.remove(f_out)

def filter_done(f, thd):
    is_alive = thd.is_alive()
    if not is_alive:
        purgfiles = r.hgetall(PURG_FILE_KEY)
        if f in purgfiles:
            # failure to remove from purgatory indicates failed conversion
            # so clean up and add to failed queue
            r.rpush(FAILED_FILE_KEY, f)
            r.hdel(PURG_FILE_KEY, f)
            (f_in, f_meta, f_out), is_diff = match_up_filenames(f, cwd)
            print(f'Remove {f_out}?')
            if os.path.exists(f_out):
                print(f'Removing {f_out}')
                os.remove(f_out)
    return is_alive

def process_next(f, cwd, hostname):
    p = psutil.Process()
    p.cpu_affinity(CPU_AFFINITY)
    print(f'Processing {f}')
    (f_in, f_meta, f_out), is_diff = match_up_filenames(f, cwd)
    info = make_uvh5_file(f_out, f_meta, f_in)
    print(f'Finished {f_in} -> {f_out}')
    times = np.unique(info['time_array'])
    starttime = Time(times[0], scale='utc', format='jd')
    stoptime = Time(times[-1], scale='utc', format='jd')
    obs_id = int(np.floor(starttime.gps))
    int_jd = int(np.floor(starttime.jd))
    if "hera-sn1" in hostname:
        prefix = os.path.join("/mnt/sn1", f"{int_jd:d}")
    elif "hera-sn2" in hostname:
        prefix = os.path.join("/mnt/sn2", f"{int_jd:d}")
    # only add sum files (and not 'junk' or 'delete' tags) to M&C
    if not is_diff and info['tag'] not in DELETE_TAGS:
        db = mc.connect_to_mc_db(None)
        with db.sessionmaker() as session:
            obs = session.get_obs(obs_id)
            if len(obs) > 0:
                print(f'observation {obs_id} for file {f} already in M&C, skipping')
            else:
                print(f'Inserting {obs_id} for file {f} in M&C')
                session.add_obs(starttime, stoptime, obs_id, info['tag'])
                session.commit()
            result = session.get_rtp_launch_record(obs_id)
            if len(result) == 0:
                print(f'Inserting {obs_id} for file {f_out} in RTP')
                session.add_rtp_launch_record(obs_id, int_jd, info['tag'],
                                              os.path.split(f_out)[-1], prefix)
                r.hset(JD_KEY, int_jd, 0)  # put this jd on a list for later rtp launch
            else:
                t0 = Time.now()
                session.update_rtp_launch_record(obs_id, t0)
                session.commit()
    r.rpush(CONV_FILE_KEY, os.path.relpath(f_out, cwd))  # document we finished it
    r.hdel(PURG_FILE_KEY, f)
    if os.path.exists(f_out):
        # check that size of f_out is reasonable
        if os.path.getsize(f_out) > MINIMUM_UVH5_RELATIVE_SIZE * os.path.getsize(f_in):
            print(f'Deleting {f_in}')
            os.remove(f_in)
    print(f'Finished')


if __name__ == '__main__':
    import multiprocessing as mp
    import redis
    import time
    import sys
    import socket

    p = psutil.Process()
    p.cpu_affinity(CPU_AFFINITY)

    assert psutil.cpu_count() == 12, "if this errors, you're not on hera-sn1"

    r = redis.Redis(REDISHOST, decode_responses=True)
    cwd = get_cwd_from_redis(r)
    hostname = socket.gethostname()
    qlen = r.llen(RAW_FILE_KEY)
    print(f'Starting conversion. Queue length={qlen}. N workers={len(CPU_AFFINITY)}')
    children = {}
    nworkers = len(CPU_AFFINITY) * 2
    try:
        while True:
            qlen = r.llen(RAW_FILE_KEY)
            children = {f: thd for f, thd in children.items()
                        if filter_done(f, thd)}
            print(f'Queue length={qlen}, N workers={len(children)}/{nworkers}')
            if qlen > 0 and len(children) < nworkers:
                # once we get a key, we commit to finish it or return it; no dropping
                f = r.rpop(RAW_FILE_KEY)  # process most recent first (LIFO)
                r.hset(PURG_FILE_KEY, f, 0)
                print(f'Starting worker on {f}')
                thd = mp.Process(target=process_next, args=(f, cwd, hostname))
                thd.start()
                children[f] = thd
            elif qlen == 0 and len(children) == 0:
                # caught up and queue is empty so check if we are done for the day
                endofday = int(r.hget('corr:files', 'ENDOFDAY'))
                if endofday:
                    # correlator has shut down and we are done, so tag out JDs
                    # that we converted files for.
                    jds = r.hgetall(JD_KEY)
                    for jd, val in jds.items():
                        if int(val) == 0:
                            r.hset(JD_KEY, jd, int(val) + 1)
                    # ENDOFDAY can now be set back to 0 whenever
                    # subsequent steps will check for jds at stage 1 or beyond
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
        return_purgatory_files(r)
