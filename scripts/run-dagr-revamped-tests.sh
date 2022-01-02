#! /bin/bash

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    -e "IMAGE_TAG=${IMAGE_TAG:-latest}" \
    -e "DISABLE_DIR_CLEANUP=TRUE" \
    --net host \
    -v "/dagr_revamped/tests/test_results:/dagr_revamped/tests/test_results" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_file_io.py

sudo rm -rv /dagr_revamped/tests/test_results

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    -e "IMAGE_TAG=${IMAGE_TAG:-latest}" \
    -e "DISABLE_DIR_CLEANUP=TRUE" \
    --net host \
    -v "/dagr_revamped/tests/test_results:/dagr_revamped/tests/test_results" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_lock_io.py

sudo rm -rv /dagr_revamped/tests/test_results

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    -e "IMAGE_TAG=${IMAGE_TAG:-latest}" \
    -e "DISABLE_DIR_CLEANUP=TRUE" \
    --net host \
    -v "/dagr_revamped/tests/test_results:/dagr_revamped/tests/test_results" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_dir_io.py

sudo rm -rv /dagr_revamped/tests/test_results