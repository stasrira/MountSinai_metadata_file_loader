import yaml

class ConfigData:

    def __init__(self, cfg_path):
    # def load_yaml_config(cfg_path):
        self.loaded = False
        with open(cfg_path, 'r') as ymlfile:
            self.cfg = yaml.load(ymlfile)
        self.loaded = True

    def get_value (self, yaml_path, delim = '/'):
        path_elems = yaml_path.split(delim)

        # loop through the path to get the required value
        val = self.cfg
        for el in path_elems:
            val = val[el]

        return val

    def getItemByKey (self, key_name):
        return str(self.get_value(key_name))

    #def get_cfg_value (cfg_ref, section, key):
        #return cfg_ref[section][key]

'''    
for section in cfg:
    print(section)
    print (cfg[section])
    print(cfg[section]['mdb_conn_str'])

#print(cfg['DB'])
#print(cfg['Logging'])
'''

#ym = load_yaml_config('E:\MounSinai\MoTrPac_API\ProgrammaticConnectivity\MountSinai_metadata_file_loader\DataFiles\study01\study.cfg_1.yaml')
#print (ym)
