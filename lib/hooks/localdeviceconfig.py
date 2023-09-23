import collections.abc

from fileloader import LocalYamlFile

from .base import BaseHook


def merge_dicts(dicts):
    """Recursively merges a series of dicts. Earlier dicts get priority."""
    output = {}
    for d in dicts:
        for k, v in d.items():
            if k not in output:
                output[k] = v
            else:
                if isinstance(v, collections.abc.Mapping) and isinstance(
                    output[k], collections.abc.Mapping
                ):
                    output[k] = merge_dicts([output[k], v])
    return output


class LocalDeviceConfig(BaseHook):
    def __init__(self, filename):
        self.config_yaml = LocalYamlFile(filename)

    async def get_device(self, clientid):
        if len(clientid) == 0:
            return None
        async with self.config_yaml as data:
            return merge_dicts([data[clientid], data["DEFAULTS"]])

    async def auth_device(self, clientid, password):
        if len(clientid) == 0:
            return None
        if len(password) == 0:
            return None
        device = await self.get_device(clientid)
        if device:
            our_password = device["password"]
            if isinstance(our_password, str):
                if password == our_password:
                    return device
            if isinstance(our_password, list):
                if password in our_password:
                    return device
        return None
