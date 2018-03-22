FROM mesosphere/spark:1.0.9-2.1.0-1-hadoop-2.6

RUN apt-get purge -y r-base* r-recommended r-doc-html \
    && apt-get -y autoremove \
    && apt-get install -y --force-yes apt-transport-https

RUN echo "deb https://cran.r-project.org/bin/linux/ubuntu trusty/" >> /etc/apt/sources.list \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 51716619E084DAB9 \
    && apt-get update \
    && apt-get install -y r-base r-base-dev

RUN Rscript -e "install.packages(c('Hmisc', 'readr', 'dplyr', 'data.table', 'vioplot', \
                                    'plotly', 'visNetwork', 'shiny'), \
                                 repo='http://cran.us.r-project.org')"
RUN apt-get -y autoremove && apt-get autoclean
