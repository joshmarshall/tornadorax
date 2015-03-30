from tornado import gen


class FooService(object):

    def __init__(self, service_url, token_callback, ioloop):
        self.service_url = service_url
        self.token_callback = token_callback

    def assert_service_url_equals(self, expected_service_url):
        if not expected_service_url == self.service_url:
            raise AssertionError("Service URL mismatch.")

    @gen.coroutine
    def assert_token_equals(self, expected_token):
        result = yield gen.Task(self.token_callback)
        if expected_token != result:
            raise AssertionError("Token mismatch.")
