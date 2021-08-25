import logging
import unittest
from pathlib import Path
from time import sleep

import requests

from tests_setup import (build_selenium, chrome_container_ctx,
                         chrome_network_ctx, run_queueman, setUpTestCase,
                         tearDownTestCase, test_containers_ctx, wait_ready)


class TestQueueMan(unittest.TestCase):

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
        setUpTestCase(self, run_queueman)
        logging.info(f"Using dir {self.results_dir}")
        logging.info(f"Using port {self.container_port}")
        selenium_dir = self.results_dir.joinpath('.selenium')
        if not selenium_dir.exists():
            selenium_dir.mkdir()
        try:
            wait_ready(self.container_port)
        except requests.exceptions.HTTPError:
            self.containerLogs()
            raise

    def test_resolve(self):
        resolved1 = None
        resolved2 = None
        resolved3 = None
        origin = f"http://0.0.0.0:{self.container_port}"

        try:
            resp1 = requests.get(f"{origin}/resolve",
                                 json={"deviant": "test-acc"})
            resp1.raise_for_status()
            resolved1 = resp1.json().get('resolved', None)

            resp2 = requests.get(
                f"{origin}/resolve/cache/query", json={"deviant": "test-acc"})
            resp2.raise_for_status()
            resp2_json = resp2.json()
            if isinstance(resp2_json, dict):
                resolved2 = resp2_json.get('result', None)['resolved']
            logging.info('Stopping queuman')
            self.container.stop()
            sleep(10)
            logging.info('Starting queuman')
            self.container.start()
            try:
                wait_ready(self.container_port)
            except requests.exceptions.HTTPError:
                self.containerLogs()
                raise
            resp3 = requests.get(
                f"{origin}/resolve/cache/query", json={"deviant": "test-acc"})
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
        endpoint = f"http://0.0.0.0:{self.container_port}/items"
        try:
            resp = requests.post(endpoint,
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

    def test_queue_urls(self):
        results = []
        endpoint = f"http://0.0.0.0:{self.container_port}/url"
        urls = [
            'https://www.deviantart.com/test-acc/gallery/all'
            'https://www.deviantart.com/test-acc/favourites/all'
        ]
        try:
            for u in urls:
                resp = requests.post(endpoint, data={'url': u})
                resp.raise_for_status()
                results.append(resp.json())
        except:
            logging.exception('Failed to queue urls')
            self.containerLogs()
            raise

        self.assertTrue(all(r == 'ok' for r in results))

    def tearDown(self):
        tearDownTestCase(self)


if __name__ == '__main__':
    build_selenium()
    with chrome_network_ctx():
        with chrome_container_ctx():
            with test_containers_ctx():
                unittest.main()
