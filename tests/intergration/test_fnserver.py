import logging
import unittest
from contextlib import contextmanager
from pathlib import Path
from time import sleep

import requests

from tests_setup import (build_selenium, create_net, run_chrome, run_fnserver,
                         setUpTestCase, tearDownTestCase)


@contextmanager
def chrome_network_ctx():
    chrome_net = create_net('chrome')
    try:
        yield chrome_net
    finally:
        chrome_net.remove()


@contextmanager
def chrome_container_ctx():
    chrome_container = run_chrome()
    try:
        yield chrome_container
    finally:
        chrome_container.stop()
        chrome_container.remove()


def wait_ready():
    try:
        resp = requests.get('http://0.0.0.0:3002/ping')
        resp.raise_for_status()
        if not resp.json == 'pong':
            raise Exception('Invalid reply')
    except (requests.exceptions.ConnectionError):
        logging.info('Fn server not ready')
        sleep(0.5)


class TestQueueMan(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.io = None
        self.container = None
        self.results_dir = None

    def containerLogs(self):
        for log_item in self.container.logs(stdout=True, stderr=True, stream=True, follow=False):
            logging.info(log_item.decode('utf-8'))

    def setUp(self):
        setUpTestCase(self, run_fnserver)
        selenium_dir = self.results_dir.joinpath('.selenium')
        if not selenium_dir.exists():
            selenium_dir.mkdir()
        sleep(3)
        # wait_ready()

    def test_queue_items(self):
        result = None
        try:
            resp = requests.post('http://0.0.0.0:3005/items',
                                    json=[
                                        {"mode": "gallery", "deviant": "test-acc"},
                                        {"mode": "favs", "deviant": "test-acc"}
                                ])
            resp.raise_for_status()
            result = resp.json()
        except:
            logging.exception('Failed to queue items')
            self.containerLogs()
            raise

        self.assertTrue(result == 'ok')


    def tearDown(self):
        tearDownTestCase(self)


if __name__ == '__main__':
    build_selenium()
    with chrome_network_ctx():
        with chrome_container_ctx():
            unittest.main()
