from distutils.core import setup
import glob
import os

ver = '0.0.1'
try:
    import subprocess
    ver = ver + '-' + subprocess.check_output(['git', 'describe', '--abbrev=8', '--always', '--dirty', '--tags']).strip()
except:
    print('Couldn\'t get version from git. Defaulting to %s' % ver)

# Generate a __version__.py file with this version in it
here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'src', '__version__.py'), 'w') as fh:
    fh.write('__version__ = "%s"' % ver)

setup(name='paper_gpu',
      version='%s' % ver,
      description='Python libraries and scripts to control the HERA correlator X-Engines',
      author='Aaron Parsons',
      author_email='aparsons@berkeley.edu',
      url='https://github.com/HERA-Team/paper_gpu',
      provides=['paper_gpu'],
      packages=['paper_gpu'],
      package_dir={'paper_gpu': 'src'},
      scripts=glob.glob('scripts/*.py')+glob.glob('scripts/*.sh'),
      )

if ver.endswith("dirty"):
    print ("********************************************") 
    print ("* You are installing from a dirty git repo *")
    print ("*      One day you will regret this.       *")
    print ("*                                          *")
    print ("*  Consider cleaning up and reinstalling.  *")
    print ("********************************************")
