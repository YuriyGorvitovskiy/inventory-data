FROM adoptopenjdk/openjdk11:jre-11.0.10_9-alpine

COPY libs/* /statemach-db/bin/

WORKDIR /statemach-db

EXPOSE 3702

CMD ["/opt/java/openjdk/bin/java", "-cp", "bin/*", "org.statemach.db.server.Main"]

