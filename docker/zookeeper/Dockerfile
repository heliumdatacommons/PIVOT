FROM ubuntu:xenial
MAINTAINER Fan Jiang <dcvan@renci.org>

RUN apt-get update \
    && apt-get install --no-install-recommends -yq zookeeperd

EXPOSE 2181

ENTRYPOINT ["/usr/share/zookeeper/bin/zkServer.sh", "start-foreground"]
