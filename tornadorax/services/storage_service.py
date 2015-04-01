import json
import hashlib

from tornado import gen
from tornado.concurrent import Future
from tornado.httpclient import AsyncHTTPClient


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
    def upload_stream(self, mimetype, writer=None, content_length=0):
        token = yield self.fetch_token()
        writer = writer or BodyWriter
        writer_instance = writer(
            self.object_url, self.container, self.name, mimetype=mimetype,
            token=token, ioloop=self.ioloop, content_length=content_length)
        raise gen.Return(writer_instance)


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


class SegmentWriter(object):

    @classmethod
    def with_segment_size(cls, chunk_size):
        def chunk_wrapper(*args, **kwargs):
            kwargs.setdefault("chunk_size", chunk_size)
            return cls(*args, **kwargs)
        return chunk_wrapper

    def __init__(
            self, url, container, object_name, token, mimetype, ioloop,
            content_length, chunk_size):
        self.url = url
        self.chunk_size = chunk_size
        self.container = container
        self.object_name = object_name
        self.token = token
        self.mimetype = mimetype
        self.content_length = content_length
        self.segment_data = ""
        self.ioloop = ioloop
        self.md5sum = hashlib.md5()
        self.segments = []
        self.current_segment_number = 0
        self.current_segment = None

    def create_segment(self):
        self.current_segment_number += 1
        segment_name = "segments/%06d" % (self.current_segment_number)
        segment_path = "/{0}/{1}/{2}".format(
            self.container, self.object_name, segment_name)

        self.segments.append({
            "path": segment_path,
            "etag": "",
            "size_bytes": 0
        })

        segment_url = "{0}/{1}".format(self.url, segment_name)
        return BodyWriter(
            segment_url, self.container, self.object_name, self.token,
            "application/video-segment", self.ioloop, content_length=0)

    @gen.coroutine
    def write(self, data):
        if not self.current_segment:
            self.current_segment = self.create_segment()

        self.segment_data += data

        while len(self.segment_data) > self.chunk_size:
            extra_bytes = self.segment_data[self.chunk_size:]
            self.segment_data = self.segment_data[:self.chunk_size]
            yield self.current_segment.write(self.segment_data)
            yield self.close_segment(self.current_segment)
            self.current_segment = self.create_segment()
            self.segment_data = extra_bytes

    @gen.coroutine
    def close_segment(self, segment):
        result = yield segment.finish()
        # verify and retry
        self.segments[-1]["etag"] = result["md5sum"]
        self.segments[-1]["size_bytes"] = result["length"]
        self.md5sum.update(result["md5sum"])
        raise gen.Return(result)

    @gen.coroutine
    def finish(self):
        if self.current_segment:
            if self.segment_data:
                yield self.current_segment.write(self.segment_data)
            yield self.close_segment(self.current_segment)

        body = json.dumps(self.segments)
        manifest_url = self.url + "?multipart-manifest=put"

        client = AsyncHTTPClient(io_loop=self.ioloop)

        headers = {
            "X-Auth-Token": self.token,
            "Content-type": self.mimetype
        }

        response = yield client.fetch(
            manifest_url, method="PUT", body=body, headers=headers)

        # verify and retry

        raise gen.Return({
            "etag": response.headers["Etag"],
            "status": "success",
            "md5sum": self.md5sum.hexdigest(),
            "length": sum([s["size_bytes"] for s in self.segments])
        })
