import fileloader

from .base import BaseHook


class LocalTokens(BaseHook):
    def __init__(self, filename):
        self.tokens_file = fileloader.get_loader().local_file(filename, text=True)

    async def get_tokens(self):
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
