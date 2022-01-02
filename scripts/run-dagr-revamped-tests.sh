#! /bin/bash

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    -e "IMAGE_TAG=${IMAGE_TAG:-latest}" \
    --net host \
    -v "/dagr_revamped/tests/test_results:/dagr_revamped/tests/test_results" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_file_io.py

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    -e "IMAGE_TAG=${IMAGE_TAG:-latest}" \
    --net host \
    -v "/dagr_revamped/tests/test_results:/dagr_revamped/tests/test_results" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_lock_io.py

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    -e "IMAGE_TAG=${IMAGE_TAG:-latest}" \
    --net host \
    -v "/dagr_revamped/tests/test_results:/dagr_revamped/tests/test_results" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_dir_io.py