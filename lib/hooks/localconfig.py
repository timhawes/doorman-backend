import logging

import fileloader
import mergedicts

from .base import BaseHook

PARENT_KEY = "profiles"
LEGACY_DEFAULTS_KEY = "DEFAULTS"


class LocalConfig(BaseHook):
    def __init__(self, devices=None, profiles=None, tokens=None):
        self.devices_file = None
        self.profiles_file = None
        self.tokens_file = None

        loader = fileloader.get_loader()
        if devices:
            self.devices_file = loader.local_file(devices, text=True)
            if profiles:
                self.profiles_file = loader.local_file(profiles, text=True)
                logging.info(
                    f"using local new-style devices and profiles files {devices}, {profiles}"
                )
            else:
                logging.info(f"using local old-style devices file {devices}")
        if tokens:
            self.tokens_file = loader.local_file(tokens, text=True)
            logging.info(f"using local tokens file {tokens}")

    async def get_device(self, clientid):
        if clientid in [None, ""]:
            # empty clientid
            return None
        if self.devices_file and self.profiles_file:
            return await self._get_device_new(clientid)
        elif self.devices_file:
            return await self._get_device_old(clientid)
        else:
            # not configured
            return None

    async def _get_device_old(self, clientid):
        if clientid == LEGACY_DEFAULTS_KEY:
            # invalid clientid
            return None
        async with self.devices_file as file:
            data = file.parse()
            return mergedicts.mergedicts([data[clientid], data[LEGACY_DEFAULTS_KEY]])

    async def _get_device_new(self, clientid):
        async with self.devices_file as d:
            try:
                device_data = d.parse()[clientid]
            except KeyError:
                return None
        if PARENT_KEY in device_data:
            profiles_data = []
            async with self.profiles_file as p:
                pdata = p.parse()
                for name in device_data[PARENT_KEY]:
                    parent_profile = mergedicts.get(pdata, name, PARENT_KEY)
                    profiles_data.append(parent_profile)
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

    async def get_tokens(self):
        if not self.tokens_file:
            # not configured
            return None
        async with self.tokens_file as file:
            return file.parse()

    async def auth_token(
        self, uid, *, groups=None, exclude_groups=None, location=None, extra={}
    ):
        groups = groups or []
        exclude_groups = exclude_groups or []

        data = await self.get_tokens()

        for username, user in data.items():
            if uid.lower() in user.get("tokens", []):
                group_matched = False
                for group in user.get("groups", []):
                    if group in exclude_groups:
                        # denied by exclude_groups
                        return {"uid": uid, "name": "", "access": 0}
                    if group in groups:
                        group_matched = True
                if group_matched or len(groups) == 0:
                    # allowed
                    return {"uid": uid, "name": username, "access": 1}
                # denied, no group match
                return {"uid": uid, "name": "", "access": 0}

        # uid not found
        return {"uid": uid, "name": "", "access": 0}
