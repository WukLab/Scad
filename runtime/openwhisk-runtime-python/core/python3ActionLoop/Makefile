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
IMG=whisk/actionloop-python-v3.7:latest
GO_RUNTIME=openwhisk-runtime-go

build:
	cp -r ../../../$(GO_RUNTIME) .
	cp -p -r ../../../lib ./libd
	-USE_RDMA=1 make -C libd clean
	cp -p -r ../../disaggrt ./disaggrt
	docker build -t $(IMG) .

clean:
	rm -rf ./$(GO_RUNTIME)
	rm -rf ./libd ./disaggrt
	docker rmi -f $(IMG)

debug: build
	docker run -p 8080:8080 \
	-ti --entrypoint=/bin/bash -v $(PWD):/mnt \
	-e OW_COMPILER=/mnt/bin/compile \
	$(IMG)

.PHONY: build clean  debug
