import os

from tornado import gen
from tornado.ioloop import IOLoop

from tornadorax.identity_client import IdentityClient


CLOUDFILES_USERNAME = os.environ["CLOUD_FILES_FETCH_USERNAME"]
CLOUDFILES_APIKEY = os.environ["CLOUD_FILES_FETCH_API_KEY"]
CHUNK_SIZE = 4096


@gen.coroutine
def list_lbs(ioloop):
    identity_url = "https://identity.api.rackspacecloud.com"
    credentials = {
        "RAX-KSKEY:apiKeyCredentials": {
            "username": CLOUDFILES_USERNAME,
            "apiKey": CLOUDFILES_APIKEY
        }
    }
    client = IdentityClient(identity_url, credentials, ioloop)
    result = yield client.authorize()
    if not result["status"] == "success":
        raise Exception("Issue authorizing: {0}".format(result))

    service = client.build_service("rax:load-balancer")
    lbs = yield service.fetch_load_balancers()
    for lb in lbs:
        print lb.name


def main():
    ioloop = IOLoop.instance()
    future = list_lbs(ioloop)
    future.add_done_callback(lambda x: ioloop.stop())
    ioloop.start()
    print "Finished."


if __name__ == "__main__":
    main()
