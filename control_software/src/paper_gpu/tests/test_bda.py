# -*- coding: utf-8 -*-
# Copyright (c) 2022 The HERA Collaboration
# Licensed under the 2-clause BSD License

from .. import bda
import pytest
import json
import yaml
import os

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
    print(corr_map)

    assert True

    return
