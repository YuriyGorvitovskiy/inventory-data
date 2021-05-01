#!/bin/sh

./gradlew build

docker build -f Dockerfile -t "yuriyg/inventory-data:0.0.6" -t "yuriyg/inventory-data:latest" ./build/

# run localy
# kubectl delete -f k8s/docker.yml
# kubectl apply -f k8s/docker.yml
#

# publish to Docker hub
# docker login --username=yuriyg
# docker push yuriyg/inventory-data:0.0.6 && docker push yuriyg/inventory-data:latest
#
