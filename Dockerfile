FROM eclipse-temurin:18-jre

ARG MODULE
ENV MODULE $MODULE
COPY /$MODULE/build/install/$MODULE/ /home/app/
WORKDIR /home/app

ENTRYPOINT bin/$MODULE --config=config.yaml
