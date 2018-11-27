FROM alpine:latest

COPY requirement.txt /tmp/requirement.txt

RUN apk add --no-cache --update python3 py3-pip \
    && pip3 install --upgrade --no-cache-dir pip \
    && pip3 install --no-cache-dir -r /tmp/requirement.txt \
    && rm -f /tmp/requirement.txt
