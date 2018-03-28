from ubuntu:latest

ARG GET_PIP_URL=https://bootstrap.pypa.io/get-pip.py

# install python3.6
RUN apt-get update \
    && apt-get install -y software-properties-common python-software-properties curl \
    && add-apt-repository -y ppa:jonathonf/python-3.6 \
    && apt-get update \
    && apt-get install -y python3.6 \
    && apt-get purge -y python3.5 python\
    && apt-get autoremove -y \
    && ln -s /usr/bin/python3.6 /usr/bin/python3 \
    && curl -O $GET_PIP_URL \
    && python3 get-pip.py \
    && rm -f get-pip.py

# install mongodb
RUN mkdir -p /data/db \
    && apt-get install -y mongodb-server

# install PIVOT
COPY . /opt/pivot
RUN pip install -r /opt/pivot/requirement.txt

# install DC/OS client
RUN [ -d /usr/local/bin ] || sudo mkdir -p /usr/local/bin \
    && curl https://downloads.dcos.io/binaries/cli/linux/x86-64/dcos-1.11/dcos -o dcos \
    && mv dcos /usr/local/bin \
    && chmod +x /usr/local/bin/dcos \
    && mkdir -p $HOME/.dcos

# Copy launch script
COPY ./docker/run.py /usr/local/bin/run.py
RUN chmod +x /usr/local/bin/run.py

# clean up
RUN apt-get -y autoremove && apt-get autoclean

ENTRYPOINT ["/usr/local/bin/run.py"]
