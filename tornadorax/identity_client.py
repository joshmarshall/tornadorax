import calendar
import json
import logging
import time

import dateutil.parser
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from tornadorax.services import service_registry
from tornadorax import utilities


LOGGER = logging.getLogger("rax:identity")


class IdentityClient(object):

    def __init__(self, identity_url, credentials, ioloop):
        self.identity_url = identity_url
        self.credentials = credentials
        self.client = AsyncHTTPClient(io_loop=ioloop)
        self.ioloop = ioloop
        self.service_catalog = None
        self.token = None
        self._token_expires = 0
        self.default_region = None

    @property
    def token_expires(self):
        return self._token_expires

    @token_expires.setter
    def token_expires(self, expiration_string):
        self._token_expires = calendar.timegm(
            dateutil.parser.parse(expiration_string).utctimetuple())

    @gen.coroutine
    def fetch_token(self):
        # FIXME: If the identity service actually goes down for any
        # measurable length of time, these operations and retries may
        # start stacking and hammering the external service far more
        # than desired. Better result would make everyone wait on a
        # single authorization, and everyone gets success / failure

        # giving a minute of buffer for slight clock skew
        if self.token_expires < time.time() + 60:
            self.token = None

        if self.token:
            raise gen.Return(self.token)

        result = yield self.authorize()
        raise gen.Return(result["token"])

    @gen.coroutine
    def authorize(self):
        full_url = self.identity_url + "/v2.0/tokens"
        body = json.dumps({"auth": self.credentials})
        headers = {"Content-type": "application/json"}

        with utilities.gen_retry(self.ioloop) as wait:
            while True:
                response = yield self.client.fetch(
                    full_url, method="POST", body=body, headers=headers,
                    raise_error=False)

                if response.code not in range(500, 600):
                    # an invalid request error or success
                    break

                LOGGER.warning(
                    "Retrying identity request ({0})".format(response.code))

                yield wait()

        if response.code > 299:
            raise gen.Return({
                "status": "error",
                "code": response.code,
                "body": response.body
            })

        auth_response = json.loads(response.body)
        self.token = auth_response["access"]["token"]["id"]
        # this will need to be augmented for other openstack providers
        self.default_region = auth_response["access"].get("user", {}).get(
            "RAX-AUTH:defaultRegion", None)
        self.token_expires = auth_response["access"]["token"]["expires"]
        self.service_catalog = auth_response["access"]["serviceCatalog"]
        raise gen.Return({"status": "success", "token": self.token})

    def build_service(self, service_type, region=None, internal=False):
        if not self.service_catalog:
            raise NoServiceCatalog(
                "No service catalog has been associated with this "
                "identity client. Try yielding authorize() first.")
        region = region or self.default_region
        return service_registry.build_service(
            service_type, self.service_catalog, ioloop=self.ioloop,
            fetch_token=self.fetch_token, region=region, internal=internal)


class NoServiceCatalog(Exception):
    pass
