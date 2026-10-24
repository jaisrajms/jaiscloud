#!/usr/bin/env bash
#
# Download the Spark jars the crypto-medallion streaming job needs, so
# stage 3 (scripts/democtl.py process) can stage them on GCS and pass them to
# the Dataproc job as jarFileUris (design §3 / SPK1 / SPK2).
#
# The deployed k3d Spark image (spark-gcs:3.5.0) ships only the GCS connector,
# so Iceberg and the Kafka source connector come from here. Jars are cached
# under spark/jars/ (gitignored) and reused when present.
#
#   demo/crypto-medallion/spark/download-jars.sh
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/jars"
BASE="https://repo1.maven.org/maven2"
mkdir -p "$DIR"

download() { # <relative-url-path> <file-name>
  local path="$1" name="$2"
  if [ -s "$DIR/$name" ]; then
    echo "cached  $name"
    return
  fi
  echo "fetch   $name"
  curl -fsSL "$BASE/$path" -o "$DIR/$name"
}

download "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar" \
  "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
download "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar" \
  "spark-sql-kafka-0-10_2.12-3.5.0.jar"
download "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar" \
  "spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
download "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
  "kafka-clients-3.4.1.jar"
download "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
  "commons-pool2-2.11.1.jar"

echo "jars in $DIR"
ls -l "$DIR"
