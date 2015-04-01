import argparse
import mimetypes
import os
import re
import sys

from tornado import gen
from tornado.ioloop import IOLoop

from tornadorax.identity_client import IdentityClient
from tornadorax.services.storage_service import SegmentWriter


CHUNK_SIZE = 64 * 1024
# default set to 50MB, maxes out at 50GB files
DEFAULT_SEGMENT_SIZE = 50 * 1024 * 1024


def get_credentials(service):
    if service == "hp":
        identity_url = "https://region-a.geo-1.identity.hpcloudsvc.com:35357/"
        credentials = {
            "tenantId": os.environ["HP_CLOUD_TENANT_ID"],
            "passwordCredentials": {
                "username": os.environ["HP_CLOUD_USERNAME"],
                "password": os.environ["HP_CLOUD_PASSWORD"]
            }
        }
    elif service == "rax":
        identity_url = "https://identity.api.rackspacecloud.com"
        credentials = {
            "RAX-KSKEY:apiKeyCredentials": {
                "username": os.environ["CLOUD_FILES_FETCH_USERNAME"],
                "apiKey": os.environ["CLOUD_FILES_FETCH_API_KEY"]
            }
        }

    return identity_url, credentials


@gen.coroutine
def upload(service, container, file_path, ioloop, segment_size):
    mimetype, _ = mimetypes.guess_type(file_path)
    base_name = re.sub("[^a-zA-Z\-_\.]", "_", os.path.basename(file_path))
    identity_url, credentials = get_credentials(service)
    client = IdentityClient(identity_url, credentials, ioloop)
    result = yield client.authorize()
    if not result["status"] == "success":
        raise Exception("Issue authorizing: {0}".format(result))

    service = client.build_service("object-store")
    container = yield service.fetch_container(container)
    obj = yield container.fetch_object(base_name)
    size = os.path.getsize(file_path)
    transferred = 0
    writer_type = SegmentWriter.with_segment_size(segment_size)
    writer = yield obj.upload_stream(
        mimetype, content_length=size, writer=writer_type)
    progress(0)
    with open(file_path, "rb") as fp:
        while True:
            data = fp.read(CHUNK_SIZE)
            if not data:
                break
            yield writer.write(data)
            percent = float(transferred) / size
            progress(percent)
            transferred += len(data)
    progress(1)
    sys.stdout.write("\n")
    sys.stdout.flush()
    result = yield writer.finish()
    print result["status"]


SPINNERS = ["\\", "|", "/", "-"]


def progress(percent):
    text = "%s%%" % (int(percent * 10000) / 100)
    total = 73
    bars = total - 3
    sys.stdout.write("\b" * total)
    chars = int(percent * bars)
    spaces = bars - chars
    spinner = SPINNERS.pop(0)
    SPINNERS.append(spinner)
    if percent == 1:
        spinner = "="

    text = " {0} ".format(text)
    if percent < .5:
        spaces = (" " * (spaces - len(text))) + text
        chars = "=" * chars
    else:
        spaces = " " * spaces
        chars = ("=" * (chars - len(text))) + text
    sys.stdout.write("|" + chars + spinner + spaces + "|")
    sys.stdout.flush()


def main():
    ioloop = IOLoop.instance()
    parser = argparse.ArgumentParser(description="Test segmented upload.")
    parser.add_argument("filepath", help="File path to upload.")
    parser.add_argument("container", help="Swift storage container.")
    parser.add_argument("--service", default="rax", dest="service")
    parser.add_argument(
        "--segment-size", default=DEFAULT_SEGMENT_SIZE, type=int,
        dest="segment_size")

    args = parser.parse_args()
    future = upload(
        args.service, args.container, args.filepath, ioloop,
        segment_size=args.segment_size)
    future.add_done_callback(lambda x: ioloop.stop())
    ioloop.start()


if __name__ == "__main__":
    main()
