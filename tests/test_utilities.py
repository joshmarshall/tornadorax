import time

import mock
from tornado.testing import AsyncTestCase, gen_test

from tornadorax import utilities


class TestUtilities(AsyncTestCase):

    @mock.patch("random.random")
    def test_generate_backoff_increases(self, mock_random):
        # ensuring it's the maximum possible way. invasive mocking...
        mock_random.return_value = 1
        results = []
        for i in range(5):
            results.append(utilities.generate_backoff(i))
        self.assertEqual([1, 2, 4, 8, 16], results)

    def test_generate_backoff_randomizes_values(self):
        value1 = utilities.generate_backoff(10)
        value2 = utilities.generate_backoff(10)
        self.assertNotEqual(value1, value2)

    @mock.patch("random.random")
    def test_generate_backoff_respects_max_wait(self, mock_random):
        mock_random.return_value = 1
        value = utilities.generate_backoff(1024)
        self.assertFalse(value > utilities.MAX_BACKOFF_WAIT_TIME)
        # now with a custom max value
        value = utilities.generate_backoff(1024, max_wait=40)
        self.assertEqual(40, value)

    @mock.patch("tornadorax.utilities.generate_backoff")
    @gen_test
    def test_retry_with_backoff(self, mock_backoff):
        mock_backoff.side_effect = [0.05 for i in range(5)]
        start = time.time()

        with utilities.gen_retry(self.io_loop) as wait:
            for i in range(5):
                yield wait()

        self.assertTrue(time.time() - start > 0.25)

    @mock.patch("tornadorax.utilities.generate_backoff")
    @gen_test
    def test_retry_with_backoff_with_max_retries(self, mock_backoff):
        mock_backoff.return_value = 0.05
        start = time.time()

        with self.assertRaises(utilities.MaxRetriesExceeded):
            with utilities.gen_retry(self.io_loop, max_retries=10) as wait:
                # providing a top end in case a test runs forever accidentally
                for i in range(20):
                    yield wait()

        # should kill itself by 10.
        self.assertTrue(time.time() - start < 0.6)
