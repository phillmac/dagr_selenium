import logging
import unittest
from contextlib import contextmanager
from pathlib import Path
from time import sleep

import requests

from tests_setup import (build_selenium, chrome_container_ctx,
                         chrome_network_ctx, run_fnserver, setUpTestCase,
                         tearDownTestCase, test_containers_ctx, wait_ready)


class TestFNServer(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.io = None
        self.container = None
        self.results_dir = None
        self.container_port = None

    def containerLogs(self):
        for log_item in self.container.logs(stdout=True, stderr=True, stream=True, follow=False):
            logging.info(log_item.decode('utf-8'))

    def setUp(self):
        setUpTestCase(self, run_fnserver)

        try:
            wait_ready(self.container_port)
        except requests.exceptions.HTTPError:
            self.containerLogs()
            raise

    def test_mkdir(self):
        pass

    def tearDown(self):
        tearDownTestCase(self)


if __name__ == '__main__':
    build_selenium()
    with chrome_network_ctx():
        with chrome_container_ctx():
            with test_containers_ctx():
                unittest.main()
