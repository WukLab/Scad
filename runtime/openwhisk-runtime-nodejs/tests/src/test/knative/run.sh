case=${1:-helloworld}
container=${2:-devnode}
docker_ip=$(docker inspect -f "{{ .NetworkSettings.IPAddress }}" $container)
	
echo "running test case $case on host $docker_ip"

echo "init code"
curl -H "Host: nodejs-helloworld.default.example.com" -d "@$case/init.json" -H "Content-Type: application/json" http://$docker_ip:8080/init
echo ""

sleep 1
echo "run code"
curl -H "Host: nodejs-helloworld.default.example.com" -d "@$case/run.json" -H "Content-Type: application/json" http://$docker_ip:8080/run
echo ""

if [ $? -ne 0 ]; then
	echo "run code exit with error"
fi
docker logs $container
