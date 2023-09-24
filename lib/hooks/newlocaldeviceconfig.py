from fileloader import LocalYamlFile, LocalJsonFile, LocalTomlFile
import mergedicts

from hooks.base import BaseHook

PARENT_KEY = "profiles"


def local_file(filename):
    if filename.endswith(".json"):
        return LocalJsonFile(filename)
    if filename.endswith(".yaml"):
        return LocalYamlFile(filename)
    if filename.endswith(".toml"):
        return LocalTomlFile(filename)
    raise ValueError(f"Unknown file type {filename}")


class LocalDeviceConfig(BaseHook):
    def __init__(self, devices_filename, profiles_filename):
        self.devices = local_file(devices_filename)
        if profiles_filename:
            self.profiles = local_file(profiles_filename)
        else:
            self.profiles = None

    async def _get_profile(self, name):
        if self.profiles is None:
            return None
        async with self.profiles as p:
            return mergedicts.get(p, name, PARENT_KEY)

    async def get_device(self, clientid):
        if len(clientid) == 0:
            return None
        async with self.devices as d:
            try:
                device_data = d[clientid]
            except KeyError:
                return None
        if PARENT_KEY in device_data:
            profiles_data = []
            async with self.profiles as p:
                profiles_data = []
                for name in device_data[PARENT_KEY]:
                    profiles_data.append(await self._get_profile(name))
            output = mergedicts.mergedicts([device_data] + profiles_data)
            del output[PARENT_KEY]
            return output
        else:
            return device_data

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
