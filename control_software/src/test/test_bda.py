from .. import bda
import json
import yaml

CORR_MAP = '../data/corr_map_example'
SNAP_CONFIG = '../data/snap_config_example.yaml'

class Test_BDA_Config(object):

    def setup_method(self):
        # load in value of redis['corr:map']
        with f as open(CORR_MAP, 'r'):
            corr_map_str = f.read()
        self.corr_map = json.loads(corr_map_str)
        # load in value of redis['snap_configuration']
        with f as open(SNAP_CONFIG, 'r'):
            config_str = f.read()
        self.config = yaml.safe_load(config_str)

    def test_parse_corr_map(self):
        corr_map = bda.get_hera_to_corr_ants(self.corr_map, self.config)
        print(corr_map)
        
        

