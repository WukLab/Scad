#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# build go proxy from source

# or build it from local, build script will copy the go folder here
FROM golang:1.15 AS builder_local
ADD ./openwhisk-runtime-go /src
RUN cd /src ; env GO111MODULE=on CGO_ENABLED=0 go build main/proxy.go && \
    mv proxy /bin/proxy

# build libd and python bindign from source
# the results will be libd.so and disagg.xxx.pythonxx.so
FROM python:3.7-buster AS build_libd
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libnanomsg-dev \
    libibverbs-dev \
    lld \
    && rm -rf /var/lib/apt/lists/*
RUN pip install \
    cython

ADD libd /libd
RUN LD_FLAGS="-fuse-ld=lld" make -C /libd ext_python

FROM python:3.7-buster
# ARG GO_PROXY_BUILD_FROM=local

# install dependencies for c libd
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libnanomsg-dev \
    libibverbs-dev \
    && rm -rf /var/lib/apt/lists/*

# Install common modules for python
RUN pip install \
    beautifulsoup4==4.6.3 \
    httplib2==0.11.3 \
    kafka_python==1.4.3 \
    lxml==4.2.5 \
    python-dateutil==2.7.3 \
    requests==2.19.1 \
    scrapy==1.5.1 \
    simplejson==3.16.0 \
    virtualenv==16.0.0 \
    twisted==18.7.0 \
    numpy==1.20.2

ADD disaggrt /disaggrt
RUN pip install /disaggrt

RUN mkdir -p /action
WORKDIR /
# COPY --from=builder_source /bin/proxy /bin/proxy_source
# COPY --from=builder_release /bin/proxy /bin/proxy_release
# RUN mv /bin/proxy_${GO_PROXY_BUILD_FROM} /bin/proxy
COPY --from=builder_local /bin/proxy /bin/proxy
# Copy python and c libds into path
COPY --from=build_libd /libd/libd.so /usr/lib/
# TODO: this may fail when we change python version
COPY --from=build_libd /libd/disagg.*.so /usr/local/lib/python3.7/lib-dynload/
ADD bin/compile /bin/compile
ADD lib/launcher.py /lib/launcher.py

# log initialization errors
ENV OW_LOG_INIT_ERROR=1
# the launcher must wait for an ack
ENV OW_WAIT_FOR_ACK=1
# using the runtime name to identify the execution environment
ENV OW_EXECUTION_ENV=openwhisk/action-python-v3.7
# compiler script
ENV OW_COMPILER=/bin/compile

ENTRYPOINT ["/bin/proxy"]

