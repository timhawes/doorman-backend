import asyncio
import logging

from tokendbformat import encode_tokendb_v1, encode_tokendb_v2, encode_tokendb_v3


class TokenAuthDatabase:
    def __init__(self, hooks):
        self.hooks = hooks
        self.data = {}
        self.lock = asyncio.Lock()

    async def load(self):
        async with self.lock:
            self.data = await self.hooks.get_tokens()

    async def get_uids(self, groups=None, exclude_groups=[]):
        await self.load()
        groups = groups or []
        uids = {}
        for username in self.data.keys():
            for group in self.data[username]["groups"]:
                if group in exclude_groups:
                    logging.info(f"excluding user {username} due to group {group}")
                elif group in groups:
                    for uid in self.data[username]["tokens"]:
                        uids[uid] = username
        return uids

    async def token_database_v1(self, groups=None, exclude_groups=[]):
        uids = self.get_uids(groups=groups, exclude_groups=exclude_groups)
        return encode_tokendb_v1(uids)

    async def token_database_v2(
        self, groups=None, hash_length=4, salt=b"", exclude_groups=[]
    ):
        uids = self.get_uids(groups=groups, exclude_groups=exclude_groups)
        return encode_tokendb_v2(uids, hash_length=hash_length, salt=salt)

    async def token_database_v3(self, groups=None, exclude_groups=[]):
        uids = self.get_uids(groups=groups, exclude_groups=exclude_groups)
        return encode_tokendb_v3(uids)

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
