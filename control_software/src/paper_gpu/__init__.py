# -*- coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-clause BSD License
"""Init file for the main paper_gpu package."""

from pkg_resources import get_distribution, DistributionNotFound

try:
    __version__ = get_distribution("paper_gpu").version
except DistributionNotFound:
    pass

from . import bda
from . import file_conversion
from . import utils
from . import catcher
