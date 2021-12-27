#! /bin/bash

docker run --rm -it \
    -v /var/run/docker.sock:/var/run/docker.sock \
    python:3.8 \
        bash -c "git clone https://github.com/phillmac/dagr_revamped.git /dagr_revamped \
        && cd /dagr_revamped \
        && pip3 install -r dev-requirements.txt \
        && pip3 install . \
        && python3 tests/intergration/test_file_io.py"

docker run --rm -it \
    -v /var/run/docker.sock:/var/run/docker.sock \
    python:3.8 \
        bash -c "git clone https://github.com/phillmac/dagr_revamped.git /dagr_revamped \
        && cd /dagr_revamped \
        && pip3 install -r dev-requirements.txt \
        && pip3 install . \
        && python3 tests/intergration/test_lock_io.py"

docker run --rm -it \
    -v /var/run/docker.sock:/var/run/docker.sock \
    python:3.8 \
        bash -c "git clone https://github.com/phillmac/dagr_revamped.git /dagr_revamped \
        && cd /dagr_revamped \
        && pip3 install -r dev-requirements.txt \
        && pip3 install . \
        && python3 tests/intergration/test_dir_io.py"