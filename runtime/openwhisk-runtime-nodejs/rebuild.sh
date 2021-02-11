./gradlew core:nodejs14Action:distDocker
docker kill devnode
docker rm devnode
docker run -d --name=devnode action-nodejs-v14
