FROM ubuntu:xenial

ENV GET_PIP_URL https://bootstrap.pypa.io/get-pip.py

# install python3.6
RUN apt-get update \
    && apt-get install -y software-properties-common curl \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y python3.6 \
    && apt-get purge -y python3.5 python\
    && apt-get autoremove -y \
    && ln -s /usr/bin/python3.6 /usr/bin/python3 \
    && curl -O $GET_PIP_URL \
    && python3 get-pip.py \
    && rm -f get-pip.py \
    && apt-get -y autoremove \
    && apt-get autoclean

COPY requirement.txt requirement.txt

RUN pip install --no-cache-dir -r requirement.txt \
    && rm -f requirement.txt
