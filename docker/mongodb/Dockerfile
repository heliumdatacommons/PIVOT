from ubuntu:xenial

# install mongodb
RUN mkdir -p /data/db \
    && apt-get update \
    && apt-get install -y mongodb-server python3\
    && apt-get -y autoremove \
    && apt-get autoclean

# Copy launch script
COPY run.py /usr/local/bin/run.py
RUN chmod +x /usr/local/bin/run.py

ENTRYPOINT ["/usr/local/bin/run.py"]
