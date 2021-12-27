#! /bin/bash

docker build . -t phillmac/dagr_revamped_tests -f dagr_revamped.dockerfile

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_file_io.py

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_lock_io.py

docker run --rm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e "TEST_IO_CLASS=${TEST_IO_CLASS:-default}" \
    phillmac/dagr_revamped_tests \
        python3 tests/intergration/test_dir_io.py"