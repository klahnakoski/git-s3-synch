# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import hashlib

from mo_dots import coalesce, listwrap
from mo_files import File
from mo_json import value2json
from mo_logs import startup, constants, Log, strings
from mo_threads import Process
from pyLibrary.aws import s3
from pyLibrary.env import http
from pyLibrary.queries import jx

CHUNK_SIZE = 8388608  # BE SURE THIS IS BOTO'S multipart_chunksize https://boto3.readthedocs.io/en/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig


def md5(source, chunk_size=CHUNK_SIZE):
    md5s = []
    for g, data in jx.groupby(source.read_bytes(), chunk_size):
        md5s.append(hashlib.md5(data).digest())

    if len(md5s) > 1:
        Log.warning("not known to work")

    new_md5 = hashlib.md5(b"".join(md5s))
    return unicode(new_md5.hexdigest()+b"-"+str(len(md5s)))


def _synch(settings):
    cache = File(settings.local_cache)
    settings.destination.directory = settings.destination.directory.trim("/")

    for repo in listwrap(coalesce(settings.repo, settings.repos)):
        name = coalesce(repo.source.name, strings.between(repo.source.url, "/", ".git"))

        if not repo.source.branch:
            Log.note("{{name}} has not branch property", name=name)
        # DO WE HAVE A LOCAL COPY?

        local_repo = File.new_instance(cache, name)
        local_dir = File.new_instance(local_repo, repo.source.directory)
        if not local_repo.exists:
            Process("clone repo", ["git", "clone", repo.source.url], cwd=local_repo).service_stopped.wait()
        # SWITCH TO BRANCH
        Process("checkout", ["git", "checkout", settings.branch], cwd=local_repo).service_stopped.wait()
        # UPDATE THE LOCAL COPY
        Process("update", ["git", "pull", "origin", settings.branch])
        # GET CURRENT LISTING OUT OF S3
        bucket = s3.Bucket(kwargs=repo.destination)
        remote_prefix = repo.directory.trim('/') + "/"
        metas = {m.key[len[remote_prefix:]]: m for m in bucket.metas(prefix=remote_prefix)}
        net_new = []
        for local_file in File.new_instance(local_repo, settings.directory).children:
            local_rel_file = local_file.abs_path[len(local_dir.abspath):]
            remote_file = metas[local_rel_file]
            if remote_file:
                if remote_file.etag != md5(local_file):
                    net_new.append(local_file)
            else:
                net_new.append(local_file)

        # SEND DIFFERENCES
        for n in net_new:
            bucket_file = settings.destination.directory+"/"+n.abspath[len(local_dir.abspath):]
            bucket.write(key=bucket_file, value=n.read_bytes(), disable_zip=True)


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)

    constants.set(settings.constants)
    path = settings.elasticsearch.host + ":" + unicode(settings.elasticsearch.port)

    try:
        _synch(settings)
    except Exception as e:
        Log.error("Problem with assign of shards", e)
    finally:
        for p, command in settings["finally"].items():
            for c in listwrap(command):
                response = http.put(
                    path + p,
                    data=value2json(c)
                )
                Log.note("Finally {{command}}\n{{result}}", command=c, result=response.all_content)

        Log.stop()


if __name__ == "__main__":
    main()

