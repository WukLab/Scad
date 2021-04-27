

all: nodejs python

nodejs:
	cd openwhisk-runtime-nodejs && \
	./gradlew :core:nodejs14Action:distDocker

python:
	cd openwhisk-runtime-python/core/python3ActionLoop && make clean build
