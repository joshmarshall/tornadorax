import json
import hashlib

from tornado import gen
from tornado.concurrent import Future
from tornado.httpclient import AsyncHTTPClient


CHUNK_SIZE = 64 * 1024
# sentinel value
READ_DONE = dict()


class StorageService(object):

    def __init__(self, service_url, fetch_token, ioloop):
        self.service_url = service_url
        self.fetch_token = fetch_token
        self.ioloop = ioloop

    @gen.coroutine
    def fetch_container(self, container_name):
        container_url = "{0}/{1}".format(self.service_url, container_name)
        container = StorageContainer(
            container_url, container_name, self.fetch_token,
            ioloop=self.ioloop)
        raise gen.Return(container)


class StorageContainer(object):

    def __init__(self, container_url, name, fetch_token, ioloop):
        self.name = name
        self.container_url = container_url
        self.fetch_token = fetch_token
        self.ioloop = ioloop

    @gen.coroutine
    def fetch_object(self, object_name):
        object_url = "{0}/{1}".format(self.container_url, object_name)
        storage_object = StorageObject(
            object_url, self.name, object_name, self.fetch_token, self.ioloop)
        raise gen.Return(storage_object)


class StorageObject(object):

    def __init__(self, url, container, object_name, fetch_token, ioloop):
        self.object_url = url
        self.container = container
        self.name = object_name
        self.fetch_token = fetch_token
        self.ioloop = ioloop
        self.client = AsyncHTTPClient(io_loop=ioloop)

    @gen.coroutine
    def info(self):
        token = yield self.fetch_token()
        headers = {"X-Auth-Token": token}
        response = yield self.client.fetch(
            self.object_url, method="HEAD", headers=headers, raise_error=False)
        if response.code >= 400:
            raise gen.Return({
                "status": "error",
                "code": response.code,
                "body": response.body
            })
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

        raise gen.Return(values)

    @gen.coroutine
    def upload_stream(self, mimetype, writer=None, content_length=0):
        token = yield self.fetch_token()
        writer = writer or BodyWriter
        writer_instance = writer(
            self.object_url, self.container, self.name, mimetype=mimetype,
            token=token, ioloop=self.ioloop, content_length=content_length)
        raise gen.Return(writer_instance)

    @gen.coroutine
    def read(self, start=0, end=0):
        body = ""
        reader = yield self.read_stream(start=start, end=end)
        for read_chunk in reader:
            body += yield read_chunk
        raise gen.Return(body)

    @gen.coroutine
    def read_stream(self, start=0, end=0):
        token = yield self.fetch_token()
        if end == 0:
            end = ""

        headers = {
            "X-Auth-Token": token,
            "Range": "bytes={0}-{1}".format(start, end)
        }

        def response_callback(response_future):
            response = response_future.result()
            if response.code >= 400:
                exception = StreamError(
                    "Error retrieving object: {0}".format(response.code))
                if not futures:
                    raise exception
                future = futures.pop(0)
                future.set_exception(exception)
            else:
                if futures:
                    futures.pop(0).set_result("")
            chunks.append(READ_DONE)

        chunks = []
        futures = []

        def body_callback(chunk):
            if futures:
                future = futures.pop(0)
                future.set_result(chunk)
            else:
                chunks.append(chunk)

        result_future = self.client.fetch(
            self.object_url, headers=headers,
            streaming_callback=body_callback, raise_error=False)
        result_future.add_done_callback(response_callback)

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
                    raise StopIteration()

                future.set_result(chunk)
                yield future

        raise gen.Return(iterate())


class BodyWriter(object):

    def __init__(
            self, url, container_name, object_name, token, mimetype,
            ioloop, content_length):
        self.content_length = content_length
        self.transferred_length = 0
        self.ioloop = ioloop
        self.md5sum = hashlib.md5()
        self.headers = {
            "X-Auth-Token": token,
            "Content-type": mimetype
        }

        if content_length > 0:
            self.headers["Content-length"] = str(content_length)

        self.client = AsyncHTTPClient(io_loop=ioloop)
        self.initialized_future = Future()
        self.finish_future = Future()
        self.request_future = self.client.fetch(
            url, method="PUT", body_producer=self, raise_error=False,
            headers=self.headers, request_timeout=0)

    @gen.coroutine
    def __call__(self, write_function):
        self.write_function = write_function
        self.initialized_future.set_result(None)
        yield self.finish_future

    @gen.coroutine
    def write(self, data):
        yield self.initialized_future
        self.md5sum.update(data)
        self.transferred_length += len(data)
        yield self.write_function(data)
        raise gen.Return(len(data))

    @gen.coroutine
    def finish(self):
        self.finish_future.set_result(None)
        response = yield self.request_future

        if response.code not in range(200, 300):
            raise gen.Return({
                "status": "error",
                "code": response.code,
                "body": response.body
            })

        raise gen.Return({
            "status": "success",
            "length": self.transferred_length,
            "md5sum": response.headers.get("ETag")
        })


# 1GB default segment size
DEFAULT_SEGMENT_SIZE = 1024 * 1024 * 1024


class SegmentWriter(object):

    @classmethod
    def with_segment_size(cls, segment_size):
        def chunk_wrapper(*args, **kwargs):
            kwargs.setdefault("segment_size", segment_size)
            return cls(*args, **kwargs)
        return chunk_wrapper

    def __init__(
            self, url, container, object_name, token, mimetype, ioloop,
            content_length, segment_size=DEFAULT_SEGMENT_SIZE):
        self.url = url
        self.segment_size = segment_size
        self.container = container
        self.object_name = object_name
        self.token = token
        self.mimetype = mimetype
        self.content_length = content_length
        self.ioloop = ioloop
        self.md5sum = hashlib.md5()
        self.segments = []
        self.segment_indexes = {}
        self.current_segment_number = 0
        self.current_segment_size = 0
        self.current_segment = None

    def create_segment(self, segment_name=None):
        if not segment_name:
            self.current_segment_number += 1
            segment_name = "%06d" % (self.current_segment_number)

        segment_name = "segments/{}".format(segment_name)
        segment_url = "{0}/{1}".format(self.url, segment_name)

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

    @gen.coroutine
    def write(self, data):
        if not self.current_segment:
            self.current_segment = self.create_segment()
            self.current_segment_size = 0

        chunk_size = \
            CHUNK_SIZE if CHUNK_SIZE < self.segment_size else self.segment_size

        if self.segment_size < self.current_segment_size + chunk_size:
            chunk_size = self.segment_size - self.current_segment_size

        if len(data) < chunk_size:
            chunk_size = len(data)

        yield self.current_segment.write(data[:chunk_size])

        remaining_data = data[chunk_size:]
        self.current_segment_size += chunk_size

        if self.current_segment_size >= self.segment_size:
            yield self.close_segment(self.current_segment)
            self.current_segment = None
            self.current_segment_size = 0

        if remaining_data:
            yield self.write(remaining_data)

    @gen.coroutine
    def close_segment(self, segment):
        result = yield segment.finish()
        # TODO: verify and retry
        segment_index = self.segment_indexes[id(segment)]
        self.segments[segment_index]["etag"] = result["md5sum"]
        self.segments[segment_index]["size_bytes"] = result["length"]
        self.md5sum.update(result["md5sum"])
        raise gen.Return(result)

    @gen.coroutine
    def finish(self, dynamic=False):
        if self.current_segment:
            yield self.close_segment(self.current_segment)

        client = AsyncHTTPClient(io_loop=self.ioloop)

        headers = {
            "X-Auth-Token": self.token,
            "Content-type": self.mimetype
        }

        body = json.dumps(self.segments)
        manifest_url = self.url + "?multipart-manifest=put"

        if dynamic:
            # much simpler, but eventually consistent and no explicitness
            headers["X-Object-Manifest"] = "{}/{}/segments".format(
                self.container, self.object_name)
            body = ""
            manifest_url = self.url

        response = yield client.fetch(
            manifest_url, method="PUT", body=body, headers=headers)

        # TODO: verify and retry

        raise gen.Return({
            "etag": response.headers["Etag"],
            "status": "success",
            "md5sum": self.md5sum.hexdigest(),
            "length": sum([s["size_bytes"] for s in self.segments])
        })


class StreamError(Exception):
    pass
