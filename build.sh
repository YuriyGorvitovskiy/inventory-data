#!/bin/sh

./gradlew build

docker build -f Dockerfile -t "yuriyg/statemach-data:1.0.0" -t "yuriyg/statemach-data:latest" ./build/

# run localy
# kubectl delete -f k8s/docker.yml
# kubectl apply -f k8s/docker.yml
#

# publish to Docker hub
docker login --username=yuriyg
docker push yuriyg/statemach-data:1.0.0 && docker push yuriyg/statemach-data:latest
#
