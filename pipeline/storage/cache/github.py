
from .abstract import AbstractCache
from github import Github, Auth
from base64 import b64decode
import json


### NOTE WELL: Git has a limit of 1000 files per directory.
### To have an actual record cache in git, we would need a PairTree

class GithubCache(AbstractCache):

    def __init__(self, config):
        if not 'tabletype' in config:
            config['tabletype'] = "cache"
        super().__init__(config)
        token = config.get('authToken', None)
        if token is not None:
            auth = Auth.Token(token)
            self.conn = Github(auth=auth)
        else:
            self.conn = Github() # anonymous read-only
        self.repo = self.conn.get_repo(config.get('repository', "project-lux/pipeline-configs"))
        self.directory = config.get('path', 'configs/subs/sources_cache')
        self.suffix = ".json"

        self.memory_cache = {}

    def _manage_key_in(self, key):
        if not key.endswith(self.suffix):
            key = key + self.suffix
        return key

    def _manage_key_out(self, key):
        if key.endswith(self.suffix):
            key = key.replace(self.suffix, '')
        return key


    def _add_to_memory(self, ct):
        # Add the github content object to our in-memory cache
        # Only do this for small caches!

    def iter_keys(self):
        cts = self.repo.get_contents(self.directory)
        for c in cts:
            yield
        return cts

    def iter_records(self):
        pass


    def get(self, key):
        key2 = self._manage_key_in(key)
        ct = self.repo.get_contents(f"{self.directory}/{key2}")
        jstr = b64decode(ct.content)
        data = json.loads(jstr)
        return {"identifier": key, "data": data, "source": self.config['name']}




class PairTreeGithubCache(GithubCache):
    pass
