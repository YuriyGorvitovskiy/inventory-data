FROM adoptopenjdk/openjdk11:jre-11.0.10_9-alpine

COPY libs/* /inventory/bin/

WORKDIR /inventory

EXPOSE 3702

CMD ["/opt/java/openjdk/bin/java", "-cp", "bin/*", "com.yg.inventory.data.service.Service"]

