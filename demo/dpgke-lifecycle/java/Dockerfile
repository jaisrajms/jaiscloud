# The submitter image the Helm chart installs (../helm, DGD4).
#
# Build context is THIS directory, after the shaded jar exists:
#
#   mvn -DskipTests package
#   docker build -t jaiscloud/dpgke-lifecycle-java:dev .
#
# DGD6 (the k3d gate) is what builds, tags and loads this image; nothing in
# DGD4 builds or publishes it, and the chart's default tag is a placeholder.
# The shade plugin already bundled every client and the META-INF/services
# entries gRPC needs, so the runtime image is a JRE and one file.
FROM eclipse-temurin:21-jre

# Numeric uid, no passwd entry and no `useradd`: the chart's pod securityContext
# pins runAsNonRoot with runAsUser 1000, and Kubernetes cannot verify a named
# USER against a non-numeric image user. /app is chowned so HOME has somewhere
# writable when a client wants a cache directory.
RUN mkdir -p /app && chown 1000:1000 /app
WORKDIR /app
COPY --chown=1000:1000 target/dpgke-lifecycle-java.jar /app/dpgke-lifecycle-java.jar

USER 1000:1000
# Without a passwd entry the JVM cannot resolve user.home; point it at the one
# directory this image owns.
ENV HOME=/app

# Configuration is entirely environment-driven (DemoConfig.java), supplied by
# the chart's ConfigMap and Secret; the app takes no arguments of its own, so
# k8s `args` from job.extraArgs are appended to this entrypoint by design.
ENTRYPOINT ["java", "-jar", "/app/dpgke-lifecycle-java.jar"]
