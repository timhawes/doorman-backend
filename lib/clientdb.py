import yamlloader


class ClientDB:

    def __init__(self, config_yaml):
        self.client_loader = yamlloader.YamlLoader(config_yaml, defaults_key='DEFAULTS')

    def authenticate(self, clientid, password):
        self.client_loader.check()
        if clientid in self.client_loader.data:
            if password == self.client_loader.data[clientid]['password']:
                return True
        return False
    
    def get_value(self, clientid, k, default=None):
        self.client_loader.check()
        return self.client_loader.data[clientid].get(k, default)

    def get_config(self, clientid):
        return self.get_value(clientid, 'config')
