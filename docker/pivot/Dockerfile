FROM heliumdatacommons/pivot-base

# install PIVOT
COPY . /opt/pivot
RUN ls /opt/pivot/log > /dev/null 2> /dev/null || mkdir /opt/pivot/log

# Copy launch script
COPY ./docker/pivot/run.py /usr/local/bin/run.py
RUN chmod +x /usr/local/bin/run.py

ENTRYPOINT ["/usr/local/bin/run.py"]
