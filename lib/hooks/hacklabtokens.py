import logging

import aiohttp
from fileloader import RemoteJsonFile

from .base import BaseHook


class HacklabTokens(BaseHook):
    def __init__(self, download_url, auth_url, api_token):
        self.download_url = download_url
        self.auth_url = auth_url

        self.headers = {"X-API-Token": api_token}
        self.tokens_file = RemoteJsonFile(
            self.download_url, headers=self.headers, min_ttl=60
        )

    async def get_tokens(self):
        async with self.tokens_file as data:
            return data

    async def auth_token(
        self, uid, *, groups=None, exclude_groups=None, location=None, extra={}
    ):
        groups = groups or []
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.auth_url,
                headers=self.headers,
                json={
                    "uid": uid,
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
