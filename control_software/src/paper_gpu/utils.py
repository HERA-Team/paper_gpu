import subprocess
import time

def run_on_hosts(hosts, cmd, user=None, wait=True):
    '''Run a command on a list of hosts.'''
    if isinstance(cmd, str):
        cmd = [cmd]
    if user is None:
        p = [subprocess.Popen(['ssh', h] + cmd) for h in hosts]
    else:
        p = [subprocess.Popen(['ssh', '%s@%s' % (user, h)] + cmd) for h in hosts]
    if wait:
        for pn in p:
            pn.wait()
    else:
        return p

def get_current_jd():
    """
    Get the current Julian date (JD) from Unix time.

    Paramaters
    ----------
    None

    Returns
    -------
    float : the current Julian date
    """
    unix_time = time.time()

    # JD 2440587.5 == Jan 1, 1970
    return (unix_time / 86400.0) + 2440587.5
