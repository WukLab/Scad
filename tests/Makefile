
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
	docker stop invoker0
	docker rm invoker0
	./run_ansible.sh teardown.yml

test: couchdb start_all apigw
	../wskish/wskish put
	../wskish/wskish post
