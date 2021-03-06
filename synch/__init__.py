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

from boto.s3 import connect_to_region
from jx_python import jx
from mo_dots import coalesce, listwrap, unwrap, Data
from mo_files import File, join_path
from mo_logs import startup, constants, Log, strings
from mo_logs.strings import quote
from mo_threads import Process

DEBUG = True
CHUNK_SIZE = 8388608  # BE SURE THIS IS BOTO'S multipart_chunksize https://boto3.readthedocs.io/en/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig


def md5(source, chunk_size=CHUNK_SIZE):
    md5s = []
    for g, data in jx.chunk(source.read_bytes(), size=chunk_size):
        md5s.append(hashlib.md5(data).digest())

    if len(md5s) == 0:
        return '"d41d8cd98f00b204e9800998ecf8427e"'
    elif len(md5s) == 1:
        return quote(md5s[0].encode("hex"))
    else:
        Log.warning("not known to work")
        new_md5 = hashlib.md5(b"".join(md5s))
        return unicode(new_md5.hexdigest()+b"-"+str(len(md5s)))


def _synch(settings):
    cache = File(settings.local_cache)
    if not cache.exists:
        cache.create()
    settings.destination.directory = settings.destination.directory.trim("/")

    for repo in listwrap(coalesce(settings.repo, settings.repos)):
        Log.alert("Synch {{repo}}", repo=repo.description)
        if not strings.between(repo.source.url, "/", ".git"):
            Log.error("This is not a git reference: {{git_url}}", git_url=repo.source.url)
        name = coalesce(repo.source.name, strings.between(repo.source.url, "/", ".git"))

        if not repo.source.branch:
            Log.note("{{name}} has not branch property", name=name)
        # DO WE HAVE A LOCAL COPY?

        local_repo = File.new_instance(cache, name)
        local_dir = File.new_instance(local_repo, repo.source.directory)
        if not local_repo.exists:
            Process("clone repo", ["git", "clone", repo.source.url, name], cwd=cache, shell=True, debug=DEBUG).join(raise_on_error=True)
        # SWITCH TO BRANCH
        Process("checkout", ["git", "checkout", repo.source.branch], cwd=local_repo, shell=True, debug=DEBUG).join(raise_on_error=True)
        # UPDATE THE LOCAL COPY
        Process("update", ["git", "pull", "origin", repo.source.branch], cwd=local_repo, shell=True, debug=DEBUG).join(raise_on_error=True)
        # GET CURRENT LISTING OUT OF S3

        try:
            connection = connect_to_region(
                region_name=repo.destination.region,
                calling_format="boto.s3.connection.OrdinaryCallingFormat",
                aws_access_key_id=unwrap(repo.destination.aws_access_key_id),
                aws_secret_access_key=unwrap(repo.destination.aws_secret_access_key)
            )
            bucket = connection.get_bucket(repo.destination.bucket)
        except Exception as e:
            Log.error("Problem connecting to {{bucket}}", bucket=repo.destination.bucket, cause=e)

        remote_prefix = repo.destination.directory.strip('/') + "/"
        listing = bucket.list(prefix=remote_prefix)
        metas = {m.key[len(remote_prefix):]: Data(key=m.key, etag=m.etag) for m in listing}
        net_new = []
        Log.note("Look for differences")
        for local_file in local_dir.leaves:
            local_rel_file = local_file.abspath[len(local_dir.abspath):].lstrip(b'/')
            if "/." in local_rel_file or local_rel_file.startswith("."):
                continue
            local_rel_file = local_rel_file.replace("qb/Qb", "qb/qb")
            remote_file = metas.get(local_rel_file)
            if not repo.force and remote_file:
                if remote_file.etag != md5(local_file):
                    net_new.append(local_file)
            else:
                net_new.append(local_file)

        # SEND DIFFERENCES
        for n in net_new:
            remote_file = join_path(repo.destination.directory, n.abspath[len(local_dir.abspath):])
            remote_file = remote_file.replace("qb/Qb", "qb/qb")
            try:
                Log.note("upload {{file}} ({{type}})", file=remote_file, type=n.mime_type)
                storage = bucket.new_key(remote_file)
                storage.content_type = n.mime_type
                storage.set_contents_from_string(n.read_bytes())
                storage.set_acl('public-read')
            except Exception as e:
                Log.warning("can not upload {{file}} ({{type}})", file=remote_file, type=n.mime_type, cause=e)


def progress(num, total):
    Log.note("Upload {{num}} of {{total}}", num=num, total=total)


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    constants.set(settings.constants)

    try:
        _synch(settings)
    except Exception as e:
        Log.error("Problem with synch", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    main()

