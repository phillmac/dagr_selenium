import logging
import unittest
from contextlib import contextmanager
from pathlib import Path
from time import sleep

import requests

from tests_setup import (build_selenium, create_net, run_chrome, run_queueman,
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
        resp = requests.get('http://0.0.0.0:3005/ping')
        resp.raise_for_status()
        if not resp.json == 'pong':
            raise Exception('Invalid reply')
    except (requests.exceptions.ConnectionError):
        logging.info('Queueman not ready')
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
        setUpTestCase(self, run_queueman)
        selenium_dir = self.results_dir.joinpath('.selenium')
        if not selenium_dir.exists():
            selenium_dir.mkdir()
        sleep(3)
        # wait_ready()

    def test_resolve(self):
        resolved1 = None
        resolved2 = None
        resolved3 = None
        try:
            resp1 = requests.get('http://0.0.0.0:3005/resolve',
                                 json={"deviant": "test-acc"})
            resp1.raise_for_status()
            resolved1 = resp1.json().get('resolved', None)

            resp2 = requests.get(
                'http://0.0.0.0:3005/resolve/cache/query', json={"deviant": "test-acc"})
            resp2.raise_for_status()
            resp2_json = resp2.json()
            if isinstance(resp2_json, dict):
                resolved2 = resp2_json.get('result', None)['resolved']
            logging.info('Stopping queuman')
            self.container.stop()
            sleep(10)
            logging.info('Starting queuman')
            self.container.start()
            sleep(3)
            resp3 = requests.get(
                'http://0.0.0.0:3005/resolve/cache/query', json={"deviant": "test-acc"})
            resp3.raise_for_status()
            resp3_json = resp3.json()
            if isinstance(resp3_json, dict):
                resolved3 = resp3_json.get('result', None)['resolved']
        except:
            logging.exception('Failed to resolve')
            self.containerLogs()
            raise

        self.assertTrue(resolved1 == 'Test-acc')
        self.assertTrue(resolved2 == 'Test-acc')
        self.assertTrue(resolved3 == 'Test-acc')


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
