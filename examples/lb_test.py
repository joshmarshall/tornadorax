import os
import sys

PATH = os.path.abspath(os.path.join(os.path.basename(__file__), "../"))
sys.path.insert(0, PATH)

from tornado import gen
from tornado.ioloop import IOLoop

from tornadorax.identity_client import IdentityClient


CLOUDFILES_USERNAME = os.environ["CLOUD_FILES_FETCH_USERNAME"]
CLOUDFILES_APIKEY = os.environ["CLOUD_FILES_FETCH_API_KEY"]
IDENTITY_URL = "https://identity.api.rackspacecloud.com"
CHUNK_SIZE = 4096
CREDENTIALS = {
    "RAX-KSKEY:apiKeyCredentials": {
        "username": CLOUDFILES_USERNAME,
        "apiKey": CLOUDFILES_APIKEY
    }
}


@gen.coroutine
def connect(ioloop):
    client = IdentityClient(IDENTITY_URL, CREDENTIALS, ioloop)
    result = yield client.authorize()
    if not result["status"] == "success":
        raise Exception("Issue authorizing: {0}".format(result))

    raise gen.Return(client.build_service("rax:load-balancer"))


@gen.coroutine
def list_lbs(ioloop):
    service = yield connect(ioloop)
    lbs = yield service.fetch_load_balancers()
    for lb in lbs:
        print lb.name


@gen.coroutine
def create_lb(ioloop):
    service = yield connect(ioloop)
    lb = yield service.create_load_balancer(
        "josh-rax-tornado-test", port=443, protocol="HTTP",
        virtual_ips=[{"type": "SERVICENET"}])
    print lb.id, lb.name


@gen.coroutine
def delete_lb(ioloop, lbid):
    service = yield connect(ioloop)
    lb = yield service.fetch_load_balancer(lbid)
    print "Delete load balancer {0} [{1}]? (y/n)".format(lb.name, lb.id)
    if sys.stdin.readline().strip() != "y":
        raise Exception("Not deleting load balancer.")
    yield service.delete_load_balancer(lbid)


def main():
    ioloop = IOLoop.instance()
    future = list_lbs(ioloop)
    future.add_done_callback(lambda x: ioloop.stop())
    ioloop.start()
    print "Finished."


if __name__ == "__main__":
    main()
