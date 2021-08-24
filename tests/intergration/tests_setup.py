import logging
from pathlib import Path
from shutil import rmtree

import docker
from dagr_revamped.config import DAGRConfig
from dagr_revamped.builtin_plugins.classes.DAGRHTTPIo import DAGRHTTPIo

logging.basicConfig(format='%(levelname)s:%(message)s', level=5)

config = DAGRConfig(
    include=[Path(__file__).parent])

client = docker.from_env()
client.images.pull('selenium/standalone-chrome')

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


def run_queueman(output_dir=None):
    return client.containers.run(
        image="phillmac/dagr_selenium",
        command=["-u", "-m", "dagr_selenium.queue_manager"],
        detach=True,
        init=True,
        environment={
            "dagr.plugins.selenium.login_policy": "prohibit",
            "dagr.plugins.selenium.create_driver_policy": "on-demand-only"
        },
        network='chrome',
        ports={
            '3005/tcp': 3005
        },
        user='root',
        volumes={
            output_dir: {"bind": output_dir, "mode": "rw"}
        },
        working_dir=output_dir
    )

def run_fnserver(output_dir=None):
    return client.containers.run(
        image="phillmac/dagr_selenium",
        command=["-u", "-m", "dagr_selenium.filenames_server"],
        detach=True,
        init=True,
        environment={
        },
        ports={
            '3002/tcp': 3002
        },
        user='root',
        volumes={
            output_dir: {"bind": output_dir, "mode": "rw"}
        },
        working_dir=output_dir
    )


def setUpTestCase(testcase, run_container):
    testcase.results_dir = Path(__file__, '../../test_results').resolve()

    if not testcase.results_dir.exists():
        testcase.results_dir.mkdir()

    testcase.container = run_container(str(testcase.results_dir))

def tearDownTestCase(testcase):
    testcase.container.stop()
    testcase.container.remove()
    rmtree(testcase.results_dir)
