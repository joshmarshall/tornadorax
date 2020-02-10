import logging
import json
import uuid

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from tornado.httpclient import AsyncHTTPClient


LOGGER = logging.getLogger("rax:queues")


class QueueService(object):

    def __init__(self, service_url, fetch_token, ioloop):
        self.service_url = service_url
        self.fetch_token = fetch_token
        self.ioloop = ioloop

    async def fetch_queue(self, queue_name):
        # TODO: check it exists, create it, or something. this
        # is pretty simple now.
        return Queue(
            self.service_url, queue_name, self.fetch_token, self.ioloop)


class Queue(object):

    def __init__(self, service_url, queue_name, fetch_token, ioloop):
        parsed_uri = urlparse.urlparse(service_url)
        self.ioloop = ioloop
        self.service_url = service_url
        self.protocol = parsed_uri.scheme
        self.fetch_token = fetch_token
        self.queue = queue_name
        self.client = AsyncHTTPClient()
        self.receive_client_id = uuid.uuid4().hex
        self.send_client_id = uuid.uuid4().hex
        self.next_url = None

    async def fetch_messages(self):
        token = await self.fetch_token()

        if not self.next_url:
            self.next_url = "{}/queues/{}/messages".format(
                self.service_url, self.queue)

        response = await self.client.fetch(
            self.next_url, headers={
                "X-Auth-Token": token,
                "Client-Id": self.receive_client_id
            }, raise_error=False)

        if response.code > 399:
            return {
                "status": "error",
                "code": response.code,
                "body": response.body
            }

        if response.code == 204:
            return {
                "status": "success",
                "messages": []
            }

        body = json.loads(response.body.decode("utf8"))
        next_urls = [l["href"] for l in body["links"] if l["rel"] == "next"]
        self.next_url = urlparse.urljoin(self.service_url, next_urls[0])
        messages = [m for m in body["messages"]]
        return {
            "status": "success",
            "messages": messages
        }

    async def push_message(self, message, ttl):
        token = await self.fetch_token()

        messages_url = "{}/queues/{}/messages".format(
            self.service_url, self.queue)
        body = json.dumps([{"ttl": ttl, "body": message}])
        response = await self.client.fetch(
            messages_url, method="POST", body=body, headers={
                "X-Auth-Token": token,
                "Client-Id": self.send_client_id}, raise_error=False)

        if response.code != 201:
            LOGGER.error(
                "Failed to push message: {} {}".format(
                    response.code, response.body))
            return {
                "status": "error",
                "code": response.code,
                "body": response.body
            }

        body = json.loads(response.body.decode("utf8"))
        return {
            "status": "success",
            "resource": body["resources"][0]
        }

    # TODO: add creation, deletion, broadcast elements, etc.
