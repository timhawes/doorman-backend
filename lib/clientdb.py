import yamlloader


class ClientDB:
    def __init__(self, config_yaml):
        self.client_loader = yamlloader.YamlLoader(config_yaml, defaults_key="DEFAULTS")

    def authenticate(self, clientid, password):
        if len(clientid) == 0:
            return False
        if len(password) == 0:
            return False
        self.client_loader.check()
        if clientid in self.client_loader.data:
            our_password = self.client_loader.data[clientid]["password"]
            if isinstance(our_password, str):
                if password == our_password:
                    return True
            if isinstance(our_password, list):
                if password in our_password:
                    return True
        return False

    def get_value(self, clientid, k, default=None):
        self.client_loader.check()
        return self.client_loader.data[clientid].get(k, default)

    def get_config(self, clientid):
        return self.get_value(clientid, "config")
