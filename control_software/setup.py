# -*- mode: python; coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-Clause BSD License

from setuptools import setup, find_namespace_packages
from setuptools_scm import get_version
import glob
import io

with io.open("README.md", "r", encoding="utf-8") as readme_file:
    readme = readme_file.read()

setup(
    name="paper_gpu",
    description="Python libraries and scripts to control the HERA correlator X-Engines",
    long_description=readme,
    long_description_content_type="text/markdown",
    license="BSD",
    author="Aaron Parsons",
    author_email="aparsons@berkeley.edu",
    url="https://github.com/HERA-Team/paper_gpu",
    packages=find_namespace_packages("src"),
    package_dir={"": "src"},
    scripts=glob.glob("scripts/*.py") + glob.glob("scripts/*.sh"),
    include_package_data=True,
    install_requires=[
        "cartopy",
        "h5py",
        "hera-mc",
        "hera-corr",
        "numpy",
        "pyuvdata",
        "pyyaml",
        "redis",
    ],
    extras_require={
        "bishuffle": [
            "hdf5plugin",
        ],
    },
)

ver = get_version(root="..", local_scheme="dirty-tag")
if ver.endswith("dirty"):
    print("********************************************")
    print("* You are installing from a dirty git repo *")
    print("*      One day you will regret this.       *")
    print("*                                          *")
    print("*  Consider cleaning up and reinstalling.  *")
    print("********************************************")
