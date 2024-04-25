import subprocess
import astropy.time import Time

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
    Get the current Julian date (JD) from astropy.

    Paramaters
    ----------
    None

    Returns
    -------
    float : the current Julian date
    """
    return Time.now().jd
