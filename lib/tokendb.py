import asyncio
import binascii
import hashlib
import logging

from tokendbformat import encode_tokendb_v1, encode_tokendb_v2, encode_tokendb_v3

# from cdbformat import encode_cdb


class TokenAuthDatabase:
    def __init__(self, hooks):
        self.hooks = hooks
        self.data = {}
        self.token_to_user = {}
        self.user_to_groups = {}
        self.md5_cache = {}
        self.lock = asyncio.Lock()

    def _md5_lookup(self, data, md5):
        for k, v in data.items():
            if k in self.md5_cache:
                pass
            else:
                self.md5_cache[k] = hashlib.md5(binascii.unhexlify(k)).hexdigest()
            if self.md5_cache[k] == md5:
                return k, v

    def _parse_data(self):
        self.token_to_user = {}
        self.user_to_groups = {}
        for user in self.data.keys():
            for token in self.data[user]["tokens"]:
                self.token_to_user[token] = user
            self.user_to_groups[user] = self.data[user]["groups"]

    async def load(self):
        async with self.lock:
            self.data = await self.hooks.get_tokens()
            self._parse_data()

    async def token_database_v1(self, groups=None, exclude_groups=[]):
        await self.load()
        groups = groups or []
        uids = {}
        for username in self.data.keys():
            for group in self.data[username]["groups"]:
                if group in exclude_groups:
                    logging.info(
                        "excluding user {} due to group {}".format(username, group)
                    )
                elif group in groups:
                    for uid in self.data[username]["tokens"]:
                        uids[uid] = True
        return encode_tokendb_v1(uids)

    async def token_database_v2(
        self, groups=None, hash_length=4, salt=b"", exclude_groups=[]
    ):
        await self.load()
        groups = groups or []
        uids = {}
        for username in self.data.keys():
            for group in self.data[username]["groups"]:
                if group in exclude_groups:
                    logging.info(
                        "excluding user {} due to group {}".format(username, group)
                    )
                elif group in groups:
                    for uid in self.data[username]["tokens"]:
                        uids[uid] = username
        return encode_tokendb_v2(uids, hash_length=hash_length, salt=salt)

    async def token_database_v3(self, groups=None, exclude_groups=[]):
        await self.load()
        groups = groups or []
        uids = {}
        for username in self.data.keys():
            for group in self.data[username]["groups"]:
                if group in exclude_groups:
                    logging.info(
                        "excluding user {} due to group {}".format(username, group)
                    )
                elif group in groups:
                    for uid in self.data[username]["tokens"]:
                        uids[uid] = username
        return encode_tokendb_v3(uids)

    # def cdb_database(self, groups=None, exclude_groups=[]):
    #     groups = groups or []
    #     uids = {}
    #     for username in self.data.keys():
    #         for group in self.data[username]['groups']:
    #             if group in exclude_groups:
    #                 logging.info('excluding user {} due to group {}'.format(username, group))
    #             elif group in groups:
    #                 for uid in self.data[username]['tokens']:
    #                     uids[uid] = username
    #     return encode_cdb(uids)

    async def is_anonymous(self, username):
        if username is None or username == "":
            return False
        # new CRM uses groups
        try:
            if "sharealike" in self.data[username]["groups"]:
                return False
            else:
                return True
        except KeyError:
            pass
        return False
