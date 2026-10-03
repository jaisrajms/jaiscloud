# syntax=docker/dockerfile:1
#
# Build one official Spring Cloud GCP sample application as a runnable image.
#
# The sample sources are cloned from the pinned upstream tag at build time, so
# this Dockerfile does not depend on a local checkout of the sample sources:
#
#   docker build \
#     --build-arg MODULE=spring-cloud-gcp-pubsub-sample \
#     -t jaiscloud-sample-pubsub:8.2.1 \
#     -f deploy/docker/gcp-samples/Dockerfile deploy/docker/gcp-samples
#
# MODULE is any directory under spring-cloud-gcp-samples/ (for example
# spring-cloud-gcp-pubsub-sample, spring-cloud-gcp-data-firestore-sample,
# spring-cloud-gcp-data-datastore-basic-sample).
#
# GIT_REF pins the upstream release. Never point it at `main`: the samples'
# parent POM resolves the released BOM from Maven Central, and `main` may carry
# SNAPSHOT versions that are not published.

ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-21
ARG JRE_IMAGE=eclipse-temurin:21-jre

FROM ${MAVEN_IMAGE} AS build
ARG MODULE
ARG GIT_REF=v8.2.1

RUN git clone --depth 1 --branch "${GIT_REF}" \
      https://github.com/GoogleCloudPlatform/spring-cloud-gcp.git /src

WORKDIR /src
# Checkstyle config paths in the samples' parent POM are relative to the repo
# root and do not resolve for a single-module build, so skip it (and tests, to
# keep the image build fast and deterministic). No BuildKit-only features are
# used here, so the build works with a buildah `docker` shim too.
RUN mvn -B -ntp \
        -DskipTests \
        -Dcheckstyle.skip=true \
        -Dmaven.javadoc.skip=true \
        -f "spring-cloud-gcp-samples/${MODULE}/pom.xml" \
        package

FROM ${JRE_IMAGE}
ARG MODULE

LABEL org.opencontainers.image.source="https://github.com/GoogleCloudPlatform/spring-cloud-gcp" \
      org.opencontainers.image.description="Spring Cloud GCP sample: ${MODULE}"

WORKDIR /app
COPY --from=build "/src/spring-cloud-gcp-samples/${MODULE}/target/" ./target/
# The Spring Boot repackage step leaves both <name>.jar (executable fat jar) and
# <name>.jar.original behind; keep only the fat jar.
RUN set -eux; \
    jar="$(find target -maxdepth 1 -name '*.jar' ! -name '*.original' | head -n1)"; \
    test -n "${jar}"; \
    mv "${jar}" app.jar; \
    rm -rf target

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
