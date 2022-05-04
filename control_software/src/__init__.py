# -*- coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-clause BSD License

from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution("paper_gpu").version
except DistributionNotFound:
    pass

from . import gpu