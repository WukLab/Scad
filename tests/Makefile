SHELL := /bin/bash

DAG ?= "2-node.json"
ANSIBLE_SCRIPT =



ifdef DIST
ANSIBLE_SCRIPT = ./run_ansible.sh d
else
ANSIBLE_SCRIPT = ./run_ansible.sh
endif



.PHONY: run_experiment

apigw:
	$(ANSIBLE_SCRIPT) apigateway.yml

analyze:
	./faas-profiler.sh WorkloadAnalyzer -p

.ONESHELL:
build_runtimes:
	if [[ -z $${DIST+x} ]]; then
		cd ../runtime && make all;
	fi

build: build_runtimes build_ow dist_docker

.ONESHELL:
build_ow:
	if [[ -z $${DIST+x} ]]; then
		cd ../openwhisk && ./gradlew build -x test
	fi

build_ow_base:
	cd ../openwhisk-base && ./gradlew distDocker


cli:
ifeq (,$(wildcard ../openwhisk/bin/wsk))
	$(ANSIBLE_SCRIPT) downloadcli-github.yml
endif

couchdb: start_couchdb initdb

controller:
	$(ANSIBLE_SCRIPT) controller.yml

.ONESHELL:
dist_docker:
	if [[ -z $${DIST+x} ]]; then
		cd ../openwhisk && ./gradlew distDocker -x test
	else
		${ANSIBLE_SCRIPT} compileOW.yml -e checkout="$$(git branch --show-current)"
	fi

edge:
	$(ANSIBLE_SCRIPT) edge.yml

initdb: wipe
	$(ANSIBLE_SCRIPT) initdb.yml

kafka:
	$(ANSIBLE_SCRIPT) kafka.yml

invoker:
	$(ANSIBLE_SCRIPT) invoker.yml -e skip_pull_runtimes=true

openwhisk:
	$(ANSIBLE_SCRIPT) openwhisk.yml -e skip_pull_runtimes=true

ow_base: build_ow_base
	rm -rf ~/.openwhisk/wsklogs/*
	cd ../openwhisk-base && git reset HEAD --hard && git apply ../ow-base.patch && ./gradlew distDocker
	./ow_base_ansible.sh properties.yml
	./ow_base_ansible.sh setup.yml
	./ow_base_ansible.sh couchdb.yml
	./ow_base_ansible.sh wipe.yml
	./ow_base_ansible.sh initdb.yml
	./ow_base_ansible.sh openwhisk.yml -e skip_pull_runtimes=true

properties:
	$(ANSIBLE_SCRIPT) properties.yml

racksched:
	$(ANSIBLE_SCRIPT) racksched.yml


reset_db: wipe
	../wskish/wskish put --file "$(DAG)"
	../wskish/wskish post

run_experiment: DAG_FILE = "$(EXPERIMENT)/dag.json"
run_experiment: WORKLOAD_FILE = "$(EXPERIMENT)/workload.json"
run_experiment:
	../wskish/wskish delete
	../wskish/wskish put --file $(DAG_FILE)
	./faas-profiler.sh WorkloadInvoker -c $(WORKLOAD_FILE)
	./faas-profiler.sh WorkloadAnalyzer -a

run_test:
	OPENWHISK_HOME="$(shell readlink -f ../openwhisk)" ./faas-profiler.sh configure.sh
	./faas-profiler.sh WorkloadInvoker -c workloads/2-node.json
	sleep 2

setup:
	$(ANSIBLE_SCRIPT) setup.yml

start_all: build teardown couchdb wipe openwhisk apigw

start_couchdb: setup
	$(ANSIBLE_SCRIPT) couchdb.yml

teardown:
	docker stop invoker0 || true
	docker stop $(shell docker ps -q) || true
	docker rm $(shell docker ps -aq) || true

test: start_all
	../wskish/wskish put --file "$(DAG)"
	../wskish/wskish post

test_ow: test run_test

test_ow_base: ow_base
	../openwhisk-base/bin/wsk -i action create action_test_function ow_base/action.js
	../openwhisk-base/bin/wsk -i action create test-action --sequence /whisk.system/action_test_function,/whisk.system/action_test_function
	./faas-profiler.sh WorkloadInvoker -c workloads/2-node-base-ow.json
	./faas-profiler.sh WorkloadAnalyzer -p

test_numpy_overhead:
	$(MAKE) py-n2r10
	echo "Finished basic python...waiting 10s for thing to settle...."
	sleep 10
	$(MAKE) py-npn2r10

py-n2r10: EXPERIMENT = experiments/python/n2-r10
py-n2r10: run_experiment

py-npn2r10: EXPERIMENT = experiments/python/np-n2-r10
py-npn2r10: run_experiment

js-n2r10: EXPERIMENT = experiments/javascript/n2-r10
js-n2r10: run_experiment


wipe:
	$(ANSIBLE_SCRIPT) wipe.yml

zk:
	$(ANSIBLE_SCRIPT) zookeeper.yml

