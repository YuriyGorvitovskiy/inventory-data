#!/bin/sh

./gradlew build

docker build -f Dockerfile -t yuriyg/inventory-data:0.0.1 ./build/

# run localy
# docker run -d -p 3702:3702 --name inventory-data yuriyg/inventory-data:0.0.1

# publish to Docker hub
# docker login --username=yuriyg 
# docker push yuriyg/inventory-data:0.0.1
