import os
from pathlib import Path
from github import Github
import requests
import io
import gzip
import tarfile
import subprocess

from ._handler import BaseHandler as BH

class CommandHandler(BH):

    def process(self, args, rest):
        # note that cfgs will be None
        if not rest:
            rest = ['.']
        where = os.path.abspath(rest[0])
        os.chdir(where)

        p = Path(where)
        p.mkdir(parents=True, exist_ok=True)
        (p / 'files').mkdir()
        (p / 'logs/flags').mkdir(parents=True)
        (p / 'indexes').mkdir()
        (p / 'input').mkdir()
        (p / 'output').mkdir()
        (p / 'system_config').mkdir()
        (p / 'temp').mkdir()

        # Try to use git clone to fetch main
        # Then it can stay in sync
        try:
            result = subprocess.run(["git", "clone", "https://github.com/project-lux/pipeline-configs.git"])
            worked = True
        except:
            worked = False
        if not (p / "pipeline-configs").exists():
            worked = False
        else:
            result = subprocess.run(["cp", "-r", "pipeline-configs/configs/system/config_cache", "system_config"])

        if not worked:
            # No git? Then download a release
            # populate the configurations from github

            (p / 'pipeline-configs').mkdir()
            g = Github()
            repo = g.get_repo('project-lux/pipeline-configs')
            rel = repo.get_latest_release()
            url = rel.tarball_url
            r = requests.get(url)
            tdata = gzip.decompress(r.content)
            tf = tarfile.TarFile(fileobj=io.BytesIO(tdata))

            for member in tf.getmembers(path="pipeline-configs"):
                cn = member.name
                cf = cn.find('configs/')
                if cf > 0:
                    member.name = cn[cf:]
                    tf.extract(member)
            subprocess.run(["mv", "pipeline-configs/configs/system/config_cache", "system_config"])

        # Now the baseline configs are there
        # Need to update base_dir on system config to p
        with open('system_config/config_cache/system.json') as fh:
            data = fh.read()
        data = data.replace('path-to-where-files-should-live', str(p))
        with open('system_config/config_cache/system.json', 'w') as fh:
            fh.write(data)

        # create a .env file pointing to the system config
        # in the current directory?
        with open('.env', 'w') as fh:
            fh.write(f"LUX_BASEPATH={p / 'system_config'}")

        print("Installed basic configuration and file system layout")
        print("Please make sure that postgres and redis are available and configured via the files in system_config/config_cache")
