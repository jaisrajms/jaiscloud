#!/usr/bin/env python3
"""Word count for the Dataproc-on-GKE lifecycle demo.

Reads a text prefix and writes "<word>\\t<count>" lines to an output prefix:

    wordcount.py <input-uri> <output-uri>

Both arguments are HCFS URIs, normally the module's buckets:

    gs://<bucket-prefix>-input/input.txt   gs://<bucket-prefix>-output/wordcount

The module uploads this file into the input bucket and points the Dataproc
job's mainPythonFileUri at it, so the code the cluster runs is the same object
Terraform created.

This is deliberately plain Spark 3.5/4 API (`SparkSession.read.text`,
`RDD.flatMap/reduceByKey`, `saveAsTextFile`) with no third-party imports: the
only environment that executes it is DGD6's k3d gate, where a real Spark image
carries the GCS connector. The module's own gate runs the emulator's mock
executor, which never starts a driver — see demo/dpgke-lifecycle/terraform/README.md.
"""

import re
import sys

from pyspark.sql import SparkSession

# Keep the tokenizer dumb and dependency-free; strip the punctuation a plain
# text file carries so the counts are stable across runs.
_WORD = re.compile(r"[A-Za-z0-9_]+")


def main(argv):
    if len(argv) != 3:
        sys.exit("usage: wordcount.py <input-uri> <output-uri>")

    input_uri, output_uri = argv[1], argv[2]

    spark = SparkSession.builder.appName("dpgke-wordcount").getOrCreate()
    try:
        lines = spark.read.text(input_uri).rdd.map(lambda row: row.value)
        counts = (
            lines.flatMap(_WORD.findall)
            .map(lambda word: (word.lower(), 1))
            .reduceByKey(lambda left, right: left + right)
        )
        # One part file keeps the demo's output easy to read; sortBy makes the
        # bytes deterministic so DGD6 can compare them.
        (
            counts.map(lambda kv: "%s\t%d" % kv)
            .sortBy(lambda line: line)
            .coalesce(1)
            .saveAsTextFile(output_uri)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv)
