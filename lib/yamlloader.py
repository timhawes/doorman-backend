import collections
import copy
import yaml
import os
import time

def dictupdate(d, u):
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = dictupdate(d.get(k, {}), v)
            d[k] = r
        else:
            d[k] = u[k]
    return d

class YamlLoader:

    data = {}
    version = 0

    def __init__(self, filename, interval=1, defaults_key=None):
        self.filename = filename
        self.interval = interval
        self.defaults_key = defaults_key
        self.current_timestamp = None
        self.last_check = 0
        self.load()

    def load(self):
        t = os.path.getmtime(self.filename)
        new_data = yaml.load(open(self.filename, 'r'))
        if self.defaults_key and self.defaults_key in new_data:
            # handle defaults and inheritance
            defaults = new_data[self.defaults_key]
            del new_data[self.defaults_key]
            new_data2 = {}
            for k, v in new_data.items():
                new_data2[k] = copy.deepcopy(defaults)
                dictupdate(new_data2[k], copy.deepcopy(new_data[k]))
            self.data = new_data2
        else:
            self.data = new_data
        self.current_timestamp = t
        self.version = t
        self.last_check = time.time()

    def check(self):
        if time.time() - self.last_check > self.interval:
            t = os.path.getmtime(self.filename)
            if t != self.current_timestamp:
                self.load()
                return True

if __name__ == '__main__':
    import time
    y = YamlLoader('test.yml', defaults_key='DEFAULTS')
    print(y.data)
    while True:
        time.sleep(1)
        if y.check():
            print(y.data)