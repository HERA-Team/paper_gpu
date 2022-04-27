# -*- mode: python; coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-Clause BSD License

from setuptools import setup
from setuptools_scm import get_version
import glob
import os

setup(
    name='paper_gpu',
    description='Python libraries and scripts to control the HERA correlator X-Engines',
    author='Aaron Parsons',
    author_email='aparsons@berkeley.edu',
    url='https://github.com/HERA-Team/paper_gpu',
    provides=['paper_gpu'],
    packages=['paper_gpu'],
    package_dir={'paper_gpu': 'src'},
    scripts=glob.glob('scripts/*.py') + glob.glob('scripts/*.sh'),
    include_package_data=True,
)

ver = get_version(root="..", local_scheme="dirty-tag")
if ver.endswith("dirty"):
    print ("********************************************") 
    print ("* You are installing from a dirty git repo *")
    print ("*      One day you will regret this.       *")
    print ("*                                          *")
    print ("*  Consider cleaning up and reinstalling.  *")
    print ("********************************************")
