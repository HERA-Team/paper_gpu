import json
import logging
import numpy as np
import redis
import yaml
import time
from astropy.time import Time, TimeDelta
from astropy import units
from hera_mc.utils import LSTScheduler
from hera_corr_cm.handlers import add_default_log_handlers
from . import bda

logger = add_default_log_handlers(logging.getLogger(__file__))

TAGS = ('delete', 'junk', 'engineering', 'science')
DEFAULT_CATCHER_HOST = 'hera-sn1'
DEFAULT_REDISHOST = 'redishost'
DEFAULT_ACCLEN = 147456 // 4  # XXX figure out where magic 4 comes from

def mcnts_per_second(sample_rate, nchan):
    """ 
    Calculate number of MCNTs in 1 second. For HERA, but not in general,
    this is the same as the number of spectra in 1 second.

    Parameters
    ----------
      sample_rate: ADC clock rate in MHz
      nchan : Number of frequency channels in 1 F-Engine spectra (prior to subselecting)

    Returns
    -------
    mcnts_per_s, Number of MCNTs in 1 second
    """
    return sample_rate / (nchan * 2)

def wait_for_catcher_boot(redishost=DEFAULT_REDISHOST, catcher_host=DEFAULT_CATCHER_HOST,
                          maxwait=60):
    r = redis.Redis(redishost, decode_responses=True)
    chan = f'hashpipe://{catcher_host}/0/status'
    t0 = time.time()
    while True:
        try:
            assert r.hexists(chan, 'CNETSTAT')
            assert r.hexists(chan, 'CNETHOLD')
            assert r.hget(chan, 'CNETSTAT') == 'holding'
            assert int(r.hget(chan, 'CNETHOLD')) == 1
            logger.info('Catcher ready.')
            break
        except(AssertionError):
            if time.time() - t0 > maxwait:
                logger.error('Catcher failed to boot.')
                raise RuntimeError(f'Maxwait={maxwait} exceeded in wait_for_catcher_boot')
            else:
                logger.info('Waiting for catcher threads to boot...')
                time.sleep(2)


def clear_redis_keys(halt=False,
                     redishost=DEFAULT_REDISHOST,
                     catcher_host=DEFAULT_CATCHER_HOST):
    '''
    '''
    r = redis.Redis(redishost, decode_responses=True)
    # Reset various statistics counters
    logger.info('Resetting Catcher redis keys')
    chan = 'hashpipe://%s/%d/set' % (catcher_host, 0)
    r.publish(chan,f'HALTOBS={int(halt)}')
    r.publish(chan, 'TRIGGER=0')
    r.publish(chan, 'MSPERFIL=0')
    for v in ['NETWAT', 'NETREC', 'NETPRC']:
        r.publish(chan, f'{v}MN=99999')
        r.publish(chan, f'{v}MX=0')
    r.publish(chan, 'MISSEDPK=0')

def release_nethold(redishost=DEFAULT_REDISHOST, catcher_host=DEFAULT_CATCHER_HOST):
    '''
    '''
    r = redis.Redis(redishost, decode_responses=True)
    chan = 'hashpipe://%s/%d/set' % (catcher_host, 0)
    logger.info('Releasing nethold (CNETHOLD=0)')
    r.publish(chan, 'CNETHOLD=0')


def set_observation(feng_sync_time_ms, start_delay=60,
                    acclen=DEFAULT_ACCLEN, xpipes=2, sample_rate=500e6,
                    nchan=8192, mcnt_xgpu_block_size=2048, slices=2,
                    redishost=DEFAULT_REDISHOST):
    '''
    Set acc_len and start_time on redis.

    Parameters
    ----------
      feng_sync_time_ms: Synchronization time in UTC milliseconds
      start_delay: delay in seconds of when to start. Default 60
      acclen: accumulated spectra per integration. Default 147456 // 4
      xpipes: number of x engines per host???
      sample_rate: sample rate of F-Engine, in Hz. Default 500e6
      nchan: number of output spectral channels. Default 8192

    Returns
    -------
    None
    '''
    assert acclen % mcnt_xgpu_block_size == 0, 'acc_len must be divisible by xgpu block size'
    file_duration_ms = int(2 * 2 * (acclen * 2) * xpipes * 2 * nchan / sample_rate * 1000)
    file_duration_s = file_duration_ms / 1000
    t = Time.now() + TimeDelta(start_delay * units.second)
    lst_time = LSTScheduler(t, file_duration_s)
    start_time = int(np.round(lst_time[0].unix)) # s
    t0 = feng_sync_time_ms / 1000 # s
    mcnt_per_s = mcnts_per_second(sample_rate, nchan)
    mcnt_delay = int((start_time - t0) * mcnt_per_s)
    # round to granularity of an integration in xGPU
    trig_mcnt = mcnt_delay - int(mcnt_delay % int(mcnt_xgpu_block_size * slices))
    trig_time = trig_mcnt / mcnt_per_s + t0
    int_time = acclen * slices * mcnt_per_s
    
    logger.debug(f'On redishost={redishost} setting:')
    logger.debug(f'    corr:acc_len = {acclen}')
    logger.debug(f'    corr:start_time = {start_time}')
    logger.debug(f'    corr:trig_mcnt = {trig_mcnt}')
    logger.debug(f'    corr:trig_time = {trig_time}')
    logger.debug(f'    corr:int_time = {int_time}')
    logger.info('Sync time: (%s)' % (time.ctime(t0)))
    logger.info('Trigger time in %.1f s (%s)' % (trig_time - time.time(),
                                               time.ctime(trig_time)))

    if redishost is not None:
        r = redis.Redis(redishost, decode_responses=True)
        r.set('corr:acc_len', str(acclen))
        r.set('corr:start_time', str(start_time))
        r.set('corr:trig_mcnt', str(trig_mcnt))
        r.set('corr:trig_time', str(trig_time))
        r.set('corr:int_time', str(int_time))
    else:
        logger.warn('No redishost provided. NOT setting redis keys.')

    return {'trig_mcnt': trig_mcnt, 'acclen': acclen,
            'ms_per_file': file_duration_ms,
            'feng_sync_time_ms': feng_sync_time_ms}

def set_xeng_output_redis_keys(trig_mcnt, acclen, redishost=DEFAULT_REDISHOST,
                         slice_by_xbox=False, slices=2, n_xeng_hosts=8,
                         mcnt_step_size=2):
    '''Use the hashpipe publish channel to update keys in all status buffers.
    See https://github.com/david-macmahon/rb-hashpipe/blob/master/bin/hashpipe_redis_gateway.rb
    
    Parameters
    ----------
        trig_mcnt:
        acclen:
        redishost:
        slice_by_xbox:
        slices:
        n_xeng_hosts:
        mcnt_step_size:

    Returns
    -------
    None
    '''
    if redishost is not None:
        rdb = redis.Redis(redishost)
    else:
        logger.warn('No redishost provided. NOT setting redis keys.')

    _msg = [f'INTCOUNT={acclen}', 'INTSTAT=start', 'OUTDUMPS=0']

    logger.debug(f'On redishost={redishost} setting:')
    if slice_by_xbox:
        for h in range(slices * n_xeng_hosts):
            host = 'px%d' % (h + 1)
            for s in range(slices):
                msg = '\n'.join([f'INTSYNC={trig_mcnt + s * mcnt_step_size}'] + _msg)
                logger.debug('    hashpipe://%s/%s/set' % (host, s), [msg])
                if redishost is not None:
                    rdb.publish('hashpipe://%s/%s/set' % (host, s), msg)
    else:
        for s in range(slices):
            msg = '\n'.join([f'INTSYNC={trig_mcnt + s * mcnt_step_size}'] + _msg)
            for h in range(n_xeng_hosts):
                host = 'px%d' % (s * n_xeng_hosts + h + 1)
                logger.debug('    hashpipe://%s/0/set' % (host), [msg])
                logger.debug('    hashpipe://%s/1/set' % (host), [msg])
                if redishost is not None:
                    rdb.publish('hashpipe://%s/0/set' % host, msg)
                    rdb.publish('hashpipe://%s/1/set' % host, msg)

def start_observing(tag, ms_per_file,
                    redishost=DEFAULT_REDISHOST, catcher_host=DEFAULT_CATCHER_HOST,
                    nants_data=192, nants=352,
                    xpipes=2):
    '''
    Set redis keys that trigger file writing on the catcher. Should
    have called set_observation() already.

    Parameters
    ----------
        tag:
        ms_per_file:
        redishost:
        catcher_host:
        nants_data:
        nants:
        xpipes:

    Returns
    -------
    None
    '''
    assert len(tag) <= 127, "Tag argument must be < 127 characters"
    if redishost is not None:
        r = redis.Redis(redishost, decode_responses=True)
        bda_config = bda.read_bda_config_from_redis(redishost)
    else:
        logger.warn('No redishost provided. NOT setting redis keys.')

    # Populate redis with the necessary metadata
    set_corr_to_hera_map(redishost=redishost)
    set_integration_bins(bda_config, redishost, catcher_host=catcher_host)

    #Configure runtime parameters
    catcher_dict = {
      'MSPERFIL' : ms_per_file,
      'SYNCTIME' : r['corr:feng_sync_time'],
      'INTTIME'  : r['corr:acc_len'],
      'TAG'      : tag,
    }

    chan = 'hashpipe://%s/%d/set' % (catcher_host, 0)
    logger.debug(f'On redishost={redishost} setting:')
    for key, val in catcher_dict.items():
        logger.debug(f'   ', chan, '%s=%s' % (key, val))
        if redishost is not None:
            r.publish(chan, f'{key}={val}')

    time.sleep(0.1) # trigger after parameters have had time to write
    logger.debug(f'   ', chan, 'TRIGGER=1')
    if redishost is not None:
        # clear end-of-day flag for this next observing session
        r.hset('corr:files', 'ENDOFDAY', 0)
        r.publish(chan, "TRIGGER=1")

def stop_observing(endofday=False, redishost=DEFAULT_REDISHOST, catcher_host=DEFAULT_CATCHER_HOST):
    '''
    '''
    clear_redis_keys(halt=True, redishost=redishost, catcher_host=catcher_host)
    rdb = redis.Redis(redishost, decode_responses=True)
    rdb.publish("hashpipe:///set", 'INTSTAT=stop')
    if endofday:
        # wait for correlator to close down file writing
        done_taking_data = False
        while not done_taking_data:
            time.sleep(1)
            done_taking_data = (rdb.hmget('corr:is_taking_data', 'state')[0] == None)
        # we can now declare observing closed for this day
        rdb.hset('corr:files', 'ENDOFDAY', 1)


def set_corr_to_hera_map(redishost=DEFAULT_REDISHOST):
    """
    Return the correlator map. Reads corr:map and snap_configuration from redis,
    and sets corr:corr_to_hera_map.

    Parameters
    ----------
    redishost : str
        The hostname of the redis server.

    Returns
    -------
    out_map : ndarray of ints
        The mapping between correlator input and HERA antenna number. The
        integer saved at a particular index `i` is the HERA ant number that
        corresponds to correlator input `i`.
    """
    r = redis.Redis(redishost)

    # A dictionary with keys which are antenna numbers
    # of the for {<ant> :{<pol>: {'host':SNAPHOSTNAME, 'channel':INTEGER}}}
    ant_to_snap = json.loads(r.hget("corr:map", "ant_to_snap"))
    config = yaml.safe_load(r.hget("snap_configuration", "config"))

    # key is correlator index, value is antenna number
    index_to_ant_map = {}

    for ant, pol in ant_to_snap.items():
        hera_ant_number = int(ant)
        try:
            pol_key = list(pol.keys())[0]
            host = pol[pol_key]["host"]
            chan = pol[pol_key]["channel"]  # runs 0-5
            snap_ant_chans = str(config['fengines'][host]['ants'])
        except(KeyError):
            snap_ant_chans = None
        if snap_ant_chans is None:
            logger.debug("Couldn't find antenna indices for %s" % host)
            continue
        corr_idx = json.loads(snap_ant_chans)[chan//2] #Indexes from 0-3 (ignores pol)
        index_to_ant_map[corr_idx] = hera_ant_number
        logger.debug("HERA antenna %d = corr input %d" % (hera_ant_number, corr_idx))

    max_corr_index = max(list(index_to_ant_map.keys()))
    out_map = np.full((max_corr_index + 1), -1)
    for corr_index, hera_ant_number in index_to_ant_map.items():
        out_map[corr_index] = hera_ant_number

    # save into redis
    # we save as one long string, with newlines to differentiate
    corr_to_hera_map_str = "\n".join([str(ant) for ant in list(out_map)])
    r.hset("corr", "corr_to_hera_map", corr_to_hera_map_str)

    return out_map

def set_integration_bins(bda_config, redishost=DEFAULT_REDISHOST, catcher_host=DEFAULT_CATCHER_HOST):
    """
    Populate redis with baseline-dependent integration information based
    on corr_to_hera_map in redis.

    Parameters
    ----------
    redishost : str, optional
        The hostname of the redis server.

    Returns
    -------
    None
    """
    # fetch other data from redis
    if redishost is not None:
        r = redis.Redis(redishost)
    else:
        logger.warn('No redishost provided. NOT setting redis keys.')

    # populate integration bin list
    integration_bin = []
    for i, t in enumerate(bda_config[:, 2]):
        if (t != 0):
           integration_bin.append(np.repeat(t, int(8 // t)))
    integration_bin = np.asarray(np.concatenate(integration_bin), dtype=np.float64)

    integration_bin_str = "\n".join([str(bl) for bl in list(integration_bin)])
    if redishost is not None:
        r.hset("corr", "integration_bin", integration_bin_str)

    # write BDA distribution to hashpipe redis
    baselines = {i: 0 for i in range(4)}
    nants = len([ant0 for ant0, ant1, t in bda_config if ant0 == ant1 and t > 0])

    for ant0, ant1, t in bda_config:
        if t == 0:
            # ignore these baselines
            continue
        n = min(int(np.log2(t)), 3)
        baselines[n] += 1

    chan = 'hashpipe://%s/%d/set' % (catcher_host, 0)
    for i, cnt in baselines.items():
        if redishost is not None:
            # XXX do these get overwritten in lines 364-368 of hera_gpu_bda_thread?
            r.publish(chan, f'NBL{2**(i+1)}SEC={cnt}')
    if redishost is not None:
        r.publish(chan, f'BDANANT={nants}')
