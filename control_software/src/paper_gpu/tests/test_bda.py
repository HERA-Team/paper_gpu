# -*- coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-clause BSD License

from .. import bda
import pytest
import json
import yaml
import os
import numpy as np

from paper_gpu.data import DATA_PATH
CORR_MAP = os.path.join(DATA_PATH, 'corr_map_example')
SNAP_CONFIG = os.path.join(DATA_PATH, 'snap_config.yaml')


@pytest.fixture(scope="function")
def config_files():
    # load in value of redis['corr:map']
    with open(CORR_MAP, 'r') as f:
        corr_map_str = f.read()
    corr_map = json.loads(corr_map_str)
    # load in value of redis['snap_configuration']
    with open(SNAP_CONFIG, 'r') as f:
        config_str = f.read()
    config = yaml.safe_load(config_str)

    yield corr_map, config

    # clean up when done
    del corr_map, config

    return

def test_parse_corr_map(config_files):
    corr_map, config = config_files
    corr_map = bda.get_hera_to_corr_ants(corr_map, config)

    # make sure the output is the type and length we expect
    assert type(corr_map) is list
    assert len(corr_map) == 143

    return

def test_assign_bl_pair_tier(config_files):
    corr_map, config = config_files
    corr_map = bda.get_hera_to_corr_ants(corr_map, config)
    bl_pairs = bda.assign_bl_pair_tier(corr_map)

    # make sure the output is the type and length we expect
    assert type(bl_pairs) is list
    for bl in bl_pairs:
        ant1, ant2, integration = bl
        if ant1 in corr_map and ant2 in corr_map:
            assert integration > 0
        else:
            assert integration == 0

    return

def test_write_bda_config_to_redis(config_files):
    corr_map, config = config_files
    corr_map = bda.get_hera_to_corr_ants(corr_map, config)
    bl_pairs = bda.assign_bl_pair_tier(corr_map)
    bda.write_bda_config_to_redis(bl_pairs)

    # check that we can get it back out again
    bl_pairs_redis = bda.read_bda_config_from_redis()
    assert np.allclose(bl_pairs_redis, bl_pairs)

    return
