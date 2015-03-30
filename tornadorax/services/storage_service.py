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
            container_url, self.fetch_token, ioloop=self.ioloop)
        raise gen.Return(container)


class StorageContainer(object):

    def __init__(self, container_url, fetch_token, ioloop):
        self.container_url = container_url
        self.fetch_token = fetch_token
        self.ioloop = ioloop

    @gen.coroutine
    def fetch_object(self, object_name):
        object_url = "{0}/{1}".format(self.container_url, object_name)
        storage_object = StorageObject(
            object_url, self.fetch_token, self.ioloop)
        raise gen.Return(storage_object)


class StorageObject(object):

    def __init__(self, object_url, fetch_token, ioloop):
        self.object_url = object_url
        self.fetch_token = fetch_token
        self.ioloop = ioloop
        self.client = AsyncHTTPClient(io_loop=ioloop)

    @gen.coroutine
    def upload_stream(self, mimetype, content_length=0):
        token = yield self.fetch_token()
        writer = BodyWriter(
            self.object_url, mimetype=mimetype, token=token,
            ioloop=self.ioloop, content_length=content_length)
        raise gen.Return(writer)


class BodyWriter(object):

    def __init__(self, url, token, mimetype, ioloop, content_length):
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
