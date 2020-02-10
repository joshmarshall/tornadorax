import json
import logging
import hashlib
import hmac

try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from tornado.concurrent import Future
from tornado.httpclient import AsyncHTTPClient


CHUNK_SIZE = 64 * 1024
# sentinel value
READ_DONE = dict()


LOGGER = logging.getLogger("rax:storage")


class StorageService(object):

    def __init__(self, service_url, fetch_token, ioloop):
        self.service_url = service_url
        self.fetch_token = fetch_token
        self.ioloop = ioloop

    async def fetch_container(self, container_name):
        LOGGER.debug("Fetching container {0}".format(container_name))
        container_url = "{0}/{1}".format(self.service_url, container_name)
        container = StorageContainer(
            container_url, container_name, self.fetch_token,
            ioloop=self.ioloop)
        return container


class StorageContainer(object):

    def __init__(self, container_url, name, fetch_token, ioloop):
        self.name = name
        self.container_url = container_url
        self.fetch_token = fetch_token
        self.ioloop = ioloop

    async def fetch_object(self, object_name, tempurl_key=None):
        LOGGER.debug("Fetching object {0}".format(object_name))
        object_url = "{0}/{1}".format(self.container_url, object_name)
        storage_object = StorageObject(
            object_url, self.name, object_name, self.fetch_token, self.ioloop,
            tempurl_key=tempurl_key)
        return storage_object


class StorageObject(object):

    def __init__(
            self, url, container, object_name, fetch_token, ioloop,
            tempurl_key=None):
        self.object_url = url
        self.container = container
        self.name = object_name
        self.fetch_token = fetch_token
        self.ioloop = ioloop
        self.tempurl_key = tempurl_key
        self.client = AsyncHTTPClient()

    def generate_tempurl(self, method, expires):
        if not self.tempurl_key:
            raise MissingTempURLKey("'tempurl_key' parameter not provided.")
        expires = int(expires)
        path = urlparse.urlparse(self.object_url).path
        base = "{0}\n{1}\n{2}".format(method, expires, path).encode("utf8")
        key = self.tempurl_key.encode("utf8")
        signature = hmac.new(key, base, hashlib.sha1).hexdigest()
        params = urlencode({
            "temp_url_expires": str(expires),
            "temp_url_sig": signature
        })
        return self.object_url + "?" + params

    async def info(self):
        LOGGER.debug("Fetching object info: {0}".format(self.object_url))
        token = await self.fetch_token()
        headers = {"X-Auth-Token": token}
        response = await self.client.fetch(
            self.object_url, method="HEAD", headers=headers, raise_error=False)
        if response.code >= 400:
            return {
                "status": "error",
                "code": response.code,
                "body": response.body
            }
        metadata = {}
        values = {}
        for header, value in response.headers.items():
            if not header.startswith("X-"):
                continue
            if header.startswith("X-Object-Meta-"):
                name = header.split("X-Object-Meta-")[1].lower()
                metadata[name] = value
            else:
                values[header.lower()] = value

        values.update({
            "status": "success",
            "metadata": metadata,
            "type": response.headers["Content-type"],
            "length": int(response.headers["Content-length"]),
            "etag": response.headers["Etag"]
        })

        return values

    async def upload_stream(
            self, mimetype, writer=None, content_length=0, metadata=None):
        LOGGER.debug("Creating upload stream for {0}".format(self.object_url))
        metadata = metadata or {}
        extra_headers = dict([
            ("X-Object-Meta-{0}".format(key), value)
            for key, value in metadata.items()
        ])
        token = await self.fetch_token()
        writer = writer or BodyWriter
        writer_instance = writer(
            self.object_url, self.container, self.name, mimetype=mimetype,
            token=token, ioloop=self.ioloop, content_length=content_length,
            extra_headers=extra_headers)
        return writer_instance

    async def read(self, start=0, end=0):
        body = bytearray()
        reader = await self.read_stream(start=start, end=end)
        for read_future in reader:
            chunk = await read_future
            body.extend(chunk)
        return body

    async def read_stream(self, start=0, end=0):
        LOGGER.debug("Creating read stream {0}".format(self.object_url))
        token = await self.fetch_token()
        if end == 0:
            end = ""

        headers = {
            "X-Auth-Token": token,
            "Range": "bytes={0}-{1}".format(start, end)
        }

        chunks = []
        futures = []

        def body_callback(chunk):
            if futures:
                future = futures.pop(0)
                future.set_result(chunk)
            else:
                chunks.append(chunk)

        def response_callback(f):
            response = f.result()
            if response.code >= 400:
                LOGGER.debug("Error reading {0} ({1})".format(
                    self.object_url, response.code))
                exception = StreamError(
                    "Error retrieving object: {0}".format(response.code))
                if not futures:
                    raise exception
                future = futures.pop(0)
                future.set_exception(exception)
            else:
                if futures:
                    futures.pop(0).set_result("")
            LOGGER.debug("Finished reading {0}".format(self.object_url))
            chunks.append(READ_DONE)

        response_future = self.client.fetch(
            self.object_url, headers=headers,
            streaming_callback=body_callback, raise_error=False)

        response_future.add_done_callback(response_callback)

        def iterate():

            while True:
                future = Future()
                if not chunks:
                    if futures:
                        raise StreamError("Previous future was not consumed.")
                    futures.append(future)
                    yield future
                    continue

                chunk = chunks.pop(0)

                if chunk is READ_DONE:
                    break

                future.set_result(chunk)
                yield future

        return iterate()


class BodyWriter(object):

    def __init__(
            self, url, container_name, object_name, token, mimetype,
            ioloop, content_length, extra_headers=None):
        self.url = url
        self.content_length = content_length
        self.transferred_length = 0
        self.ioloop = ioloop
        self.md5sum = hashlib.md5()
        self.headers = {
            "X-Auth-Token": token,
            "Content-type": mimetype
        }
        self.headers.update(extra_headers or {})

        if content_length > 0:
            self.headers["Content-length"] = str(content_length)

        self.client = AsyncHTTPClient()
        self.initialized_future = Future()
        self.finish_future = Future()
        self.request_future = self.client.fetch(
            url, method="PUT", body_producer=self.body_producer,
            raise_error=False, headers=self.headers)

    async def body_producer(self, write_function):
        LOGGER.debug("Starting transfer to {0}".format(self.url))
        self.write_function = write_function
        self.initialized_future.set_result(None)
        await self.finish_future

    async def write(self, data):
        await self.initialized_future
        self.md5sum.update(data)
        self.transferred_length += len(data)
        await self.write_function(data)
        LOGGER.debug("Sent {0} to {1}".format(
            self.transferred_length, self.url))
        return len(data)

    async def finish(self):
        self.finish_future.set_result(None)
        LOGGER.debug("Closing file: {}".format(self.url))

        response = await self.request_future

        if response.code not in range(200, 300):
            LOGGER.debug("File transfer {0} failed: {1}".format(
                self.url, response.code))
            return {
                "status": "error",
                "code": response.code,
                "body": response.body
            }

        LOGGER.debug("Finished file: {}".format(self.url))

        return {
            "status": "success",
            "length": self.transferred_length,
            "md5sum": response.headers.get("ETag")
        }


# 1GB default segment size
DEFAULT_SEGMENT_SIZE = 1024 * 1024 * 1024


class SegmentWriter(object):

    @classmethod
    def with_defaults(cls, **cls_kwargs):
        def chunk_wrapper(*args, **kwargs):
            kwargs.update(cls_kwargs)
            return cls(*args, **kwargs)
        return chunk_wrapper

    def __init__(
            self, url, container, object_name, token, mimetype, ioloop,
            content_length, segment_size=DEFAULT_SEGMENT_SIZE, dynamic=False,
            extra_headers=None):
        self.url = url
        self.segment_size = segment_size
        self.container = container
        self.object_name = object_name
        self.token = token
        self.dynamic = dynamic
        self.mimetype = mimetype
        self.content_length = content_length
        self.ioloop = ioloop
        self.md5sum = hashlib.md5()
        self.segments = []
        self.segment_indexes = {}
        self.current_segment_number = 0
        self.current_segment_size = 0
        self.current_segment = None
        self.extra_headers = extra_headers or {}

    def create_segment(self, segment_name=None):
        if not segment_name:
            self.current_segment_number += 1
            segment_name = "%06d" % (self.current_segment_number)

        segment_name = "segments/{}".format(segment_name)
        segment_url = "{0}/{1}".format(self.url, segment_name)

        LOGGER.debug("Creating new segment {0}".format(segment_url))

        segment = BodyWriter(
            segment_url, self.container, self.object_name, self.token,
            "application/video-segment", self.ioloop, content_length=0)

        segment_path = "/{0}/{1}/{2}".format(
            self.container, self.object_name, segment_name)

        self.segment_indexes[id(segment)] = len(self.segments)

        self.segments.append({
            "path": segment_path,
            "etag": "",
            "size_bytes": 0
        })

        return segment

    async def write(self, data):
        if not self.current_segment:
            self.current_segment = self.create_segment()
            self.current_segment_size = 0

        chunk_size = \
            CHUNK_SIZE if CHUNK_SIZE < self.segment_size else self.segment_size

        if self.segment_size < self.current_segment_size + chunk_size:
            chunk_size = self.segment_size - self.current_segment_size

        if len(data) < chunk_size:
            chunk_size = len(data)

        await self.current_segment.write(data[:chunk_size])

        remaining_data = data[chunk_size:]
        self.current_segment_size += chunk_size

        if self.current_segment_size >= self.segment_size:
            await self.close_segment(self.current_segment)
            self.current_segment = None
            self.current_segment_size = 0

        if remaining_data:
            await self.write(remaining_data)

    async def close_segment(self, segment):
        result = await segment.finish()
        # TODO: verify and retry
        segment_index = self.segment_indexes[id(segment)]
        self.segments[segment_index]["etag"] = result["md5sum"]
        self.segments[segment_index]["size_bytes"] = result["length"]
        self.md5sum.update(result["md5sum"].encode("utf8"))
        return result

    async def finish(self):
        if self.current_segment:
            await self.close_segment(self.current_segment)

        client = AsyncHTTPClient()

        headers = {
            "X-Auth-Token": self.token,
            "Content-type": self.mimetype
        }

        headers.update(self.extra_headers)

        if self.dynamic:
            # dynamic manifests are much simpler, since they simply contain
            # a pattern to match, but they are eventually consistent with
            # updates and the segmentation is not explicit (meaning
            # that extra / duplicate segments are possible).
            headers["X-Object-Manifest"] = "{}/{}/segments".format(
                self.container, self.object_name)
            body = ""
            manifest_url = self.url
        else:
            # static manifests are more stable, since they include
            # explicit information about the chunks, but they do have size
            # limitations
            body = json.dumps(self.segments)
            manifest_url = self.url + "?multipart-manifest=put"

        response = await client.fetch(
            manifest_url, method="PUT", body=body, headers=headers)

        LOGGER.debug("Finished segmented delivery {0}".format(self.url))

        # TODO: verify and retry

        return {
            "etag": response.headers["Etag"],
            "status": "success",
            "md5sum": self.md5sum.hexdigest(),
            "length": sum([s["size_bytes"] for s in self.segments])
        }


class StreamError(Exception):
    pass


class MissingTempURLKey(Exception):
    pass
