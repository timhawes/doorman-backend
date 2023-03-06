import binascii
import hashlib
import logging

import aiohttp
from tokendbformat import encode_tokendb_v1, encode_tokendb_v2

# from cdbformat import encode_cdb


class TokenAuthDatabase:
    def __init__(self, download_url, query_url, auth_url, api_token):
        self.download_url = download_url
        self.query_url = query_url
        self.auth_url = auth_url
        self.headers = {"X-API-Token": api_token}
        self.data = {}
        self.token_to_user = {}
        self.user_to_groups = {}
        self.md5_cache = {}

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
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    self.download_url, headers=self.headers
                ) as response:
                    self.data = await response.json()
                    self._parse_data()
            except aiohttp.client_exceptions.ClientError:
                if self.data:
                    logging.exception("ignoring exception while re-loading data")
                else:
                    raise

    async def auth_token(
        self,
        uid,
        counter=None,
        groups=None,
        online=True,
        location=None,
        exclude_groups=[],
    ):
        if online:
            return await self.auth_token_hex_online(
                uid,
                counter=counter,
                groups=groups,
                location=location,
                exclude_groups=exclude_groups,
            )
        else:
            return await self.auth_token_hex_offline(
                uid,
                counter=counter,
                groups=groups,
                location=location,
                exclude_groups=exclude_groups,
            )

    async def auth_token_hex_offline(
        self, uid, counter=None, groups=None, location=None, exclude_groups=[]
    ):
        groups = groups or []
        if uid in self.token_to_user:
            username = self.token_to_user[uid]
            # TODO: check exclude_groups
            for group in groups:
                if group in self.user_to_groups[user]:
                    logging.info(
                        "token {} -> user {} -> group {} ok (database auth)".format(
                            uid, username, group
                        )
                    )
                    return {"uid": uid, "name": username, "access": 1}
        logging.info("token {} -> user {} -> group not matched".format(uid, username))
        return {"uid": uid, "name": username, "access": 0}

    async def auth_token_hex_online(
        self, uid, counter=None, groups=None, location=None, exclude_groups=[]
    ):
        groups = groups or []
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.auth_url,
                headers=self.headers,
                json={
                    "uid": uid,
                    "counter": counter,
                    "groups": groups,
                    "exclude_groups": exclude_groups,
                    "location": location,
                },
            ) as response:
                try:
                    response = await response.json()
                except aiohttp.client_exceptions.ContentTypeError:
                    return None
        if response.get("found") is True:
            if response.get("authorized") is True:
                username = response["username"]
                logging.info(
                    "token {} -> user {} -> ok (online auth)".format(uid, username)
                )
                return {"uid": uid, "name": username, "access": 1}
            else:
                logging.info(
                    "token {} -> not authorized -> {} (online auth)".format(
                        uid, response.get("reason")
                    )
                )
                return {"uid": uid, "name": "", "access": 0}
        else:
            logging.info(
                "token {} -> not found -> {} (online auth)".format(
                    uid, response.get("reason")
                )
            )
            return {"uid": uid, "name": "", "access": 0}
        # for group in groups:
        #    if group in response['groups']:
        #        logging.info('token {} -> user {} -> group {} ok (online auth)'.format(uid, username, group))
        #        return {'uid': uid, 'name': username, 'access': 1}
        # logging.info('token {} -> user {} -> group not matched'.format(uid, username))
        # return {'uid': uid, 'name': username, 'access': 0}

    def token_database_v1(self, groups=None, exclude_groups=[]):
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

    def token_database_v2(
        self, groups=None, hash_length=4, salt=b"", exclude_groups=[]
    ):
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
        # old CRM uses privacy attribute
        try:
            return self.data[username]["privacy"]
        except KeyError:
            pass
        # new CRM uses groups
        if "sharealike" in self.data[username]["groups"]:
            return False
        else:
            return True
