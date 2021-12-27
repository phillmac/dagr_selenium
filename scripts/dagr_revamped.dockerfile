FROM python:3.8

WORKDIR /dagr_revamped

RUN git clone https://github.com/phillmac/dagr_revamped.git . \
        && pip3 install -r dev-requirements.txt \
        && pip3 install .
