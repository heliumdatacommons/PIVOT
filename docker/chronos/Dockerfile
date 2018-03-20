FROM ubuntu:xenial
MAINTAINER Fan Jiang <dcvan@renci.org>

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF \
    && echo "deb http://repos.mesosphere.io/debian jessie main" >> /etc/apt/sources.list.d/mesosphere.list \
    && apt-get update \
    && apt-get install --no-install-recommends -yq mesos git openjdk-8-jdk python-dev curl iproute2 maven

RUN curl -sL https://deb.nodesource.com/setup_8.x -o nodesource_setup.sh
RUN bash nodesource_setup.sh && rm -rf nodesource_setup.sh
RUN apt-get install -yq nodejs
RUN git clone https://github.com/dcvan24/chronos.git
RUN cd chronos && mvn package -DskipTests 
RUN cd .. && cp -a chronos/target/chronos*.jar $HOME && rm -rf chronos
RUN rm -rf $HOME/.m2
RUN apt-get purge -y maven git && apt-get autoremove -y && apt-get autoclean

COPY run.sh /usr/bin/run.sh
RUN chmod +x /usr/bin/run.sh

ENTRYPOINT ["/usr/bin/run.sh"]
