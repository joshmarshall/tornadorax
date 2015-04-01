from tornado import gen


def make_fetch_token(token):

    @gen.coroutine
    def fetch_token():
        raise gen.Return(token)

    return fetch_token


fetch_token = make_fetch_token("TOKEN")
