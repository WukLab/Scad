
build_runtimes:
	cd ../runtime && make all

build_ow:
	cd ../openwhisk && ./gradlew build -x test

build: build_runtimes build_ow

dist_docker: build
	cd ../openwhisk && ./gradlew distDocker

setup:
	./run_ansible.sh setup.yml

start_couchdb: setup
	./run_ansible.sh couchdb.yml

initdb: wipe
	./run_ansible.sh initdb.yml

wipe:
	./run_ansible.sh wipe.yml

couchdb: start_couchdb initdb

start_all:
	./run_ansible.sh openwhisk.yml -e skip_pull_runtimes=true

controller:
	./run_ansible.sh controller.yml

racksched:
	./run_ansible.sh racksched.yml

invoker:
	./run_ansible.sh invoker.yml -e skip_pull_runtimes=true

apigw:
	./run_ansible.sh apigateway.yml

kafka:
	./run_ansible.sh kafka.yml

zk:
	./run_ansible.sh zookeeper.yml

teardown:
	docker stop invoker0 || true
	docker stop $(shell docker ps -aq)
	docker rm $(shell docker ps -aq)

run_profiler:
	../wskish/wskish delete
	../wskish/wskish put --file 2-node.json
	./faas-profiler.sh WorkloadInvoker -c zac-test.json
	./faas-profiler.sh WorkloadAnalyzer -p

test: build teardown couchdb wipe start_all apigw
	../wskish/wskish put --file 2-node.json
	../wskish/wskish post

build_ow_base:
	cd ../openwhisk-base && ./gradlew distDocker

ow_base: build_ow
	cd ../openwhisk-base && git apply ../ow-base.patch
	./ow_base_ansible.sh setup.yml
	./ow_base_ansible.sh couchdb.yml
	./ow_base_ansible.sh wipe.yml
	./ow_base_ansible.sh initdb.yml
	./ow_base_ansible.sh openwhisk.yml -e skip_pull_runtimes=true
