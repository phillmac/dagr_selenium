import logging
import random
import string
from contextlib import contextmanager
from pathlib import Path
from shutil import rmtree
from time import sleep

import docker
import requests
from dagr_revamped.builtin_plugins.classes.DAGRHTTPIo import DAGRHTTPIo
from dagr_revamped.config import DAGRConfig

logging.basicConfig(format='%(levelname)s:%(message)s', level=5)

config = DAGRConfig(
    include=[Path(__file__).parent])

client = docker.from_env()
client.images.pull('selenium/standalone-chrome')

test_containers = []


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


@contextmanager
def test_containers_ctx():
    try:
        yield
    finally:
        for c in test_containers:
            try:
                c.stop()
                c.remove()
            except:
                logging.exception('Unable to cleanup container')


def wait_ready(port):
    waiting = True
    while waiting:
        try:
            resp = requests.get(f"http://0.0.0.0:{port}/ping", timeout=3)
            resp.raise_for_status()
            if not resp.json() == 'pong':
                raise Exception('Invalid reply')
        except (requests.exceptions.ConnectionError):
            logging.info('Container not ready')
            sleep(1)
        else:
            waiting = False
    logging.info('Ready')


def create_net(name):
    return client.networks.create(name)


def run_chrome():
    return client.containers.run(
        image="selenium/standalone-chrome",
        name="chrome",
        detach=True,
        environment={},
        init=True,
        user='root',
        volumes={
            '/dev/shm': {"bind": "/dev/shm", "mode": "rw"}
        },
        network='chrome'
    )


def build_selenium():
    return client.images.build(
        path="/workspace/dagr_selenium",
        tag="phillmac/dagr_selenium",
        rm=True,
        pull=True
    )


def run_queueman(port, output_dir=None):
    return client.containers.run(
        image="phillmac/dagr_selenium",
        command=["-u", "-m", "dagr_selenium.queue_manager"],
        detach=True,
        init=True,
        environment={
            "dagr.plugins.selenium.login_policy": "prohibit",
            "dagr.plugins.selenium.create_driver_policy": "on-demand-only",
            "QUEUEMAN_LISTEN_PORT": port
        },
        network='chrome',
        ports={
            f"{port}/tcp": port
        },
        user='root',
        volumes={
            output_dir: {"bind": output_dir, "mode": "rw"}
        },
        working_dir=output_dir
    )


def run_fnserver(port, output_dir=None):
    return client.containers.run(
        image="phillmac/dagr_selenium",
        command=["-u", "-m", "dagr_selenium.filenames_server"],
        detach=True,
        init=True,
        environment={
            "FILENAME_SERVER_LISTEN_PORT": port
        },
        ports={
            f"{port}/tcp": port
        },
        user='root',
        volumes={
            output_dir: {"bind": output_dir, "mode": "rw"}
        },
        working_dir=output_dir
    )


def setUpTestCase(testcase, run_container):
    random_name = ''.join(random.choices(
        string.ascii_uppercase + string.digits, k=5))
    testcase.container_port = random.randint(1025, 9999)

    testcase.results_dir = Path(
        __file__, '../../test_results', random_name).resolve()
    testcase.results_dir.mkdir()

    testcase.container = run_container(
        testcase.container_port, str(testcase.results_dir))
    test_containers.append(testcase.container)


def tearDownTestCase(testcase):
    testcase.container.stop()
    testcase.container.remove()
    test_containers.remove(testcase.container)
    rmtree(testcase.results_dir)
