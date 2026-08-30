#!/usr/bin/env python3
"""
Seed test resources for account 111111111111 in a running JaisCloud instance.

JaisCloud identity rule: a 12-digit numeric AWS_ACCESS_KEY_ID is used as-is
as the account ID (see internal/aws/identity/identity.go AccountFromAccessKey).

Usage:
  python3 scripts/seed_account2.py [--endpoint http://localhost:4566]
"""

import argparse
import json
import sys
import zipfile
import io

import boto3
from botocore.config import Config as BotoConfig

ACCOUNT = "111111111111"
REGION  = "us-east-1"

LAMBDA_CODE = b"""
import json

def handler(event, context):
    print(f"account-2 function invoked with event: {json.dumps(event)}")
    return {"statusCode": 200, "account": "111111111111", "event": event}
"""


def make_zip(code: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("handler.py", code)
    return buf.getvalue()


def session(endpoint: str) -> boto3.Session:
    return boto3.Session(
        aws_access_key_id=ACCOUNT,       # 12-digit → account ID in JaisCloud
        aws_secret_access_key="test",
        region_name=REGION,
    )


def sqs_client(endpoint, sess):
    return sess.client("sqs", endpoint_url=endpoint,
                       config=BotoConfig(retries={"max_attempts": 1}))


def lambda_client(endpoint, sess):
    return sess.client("lambda", endpoint_url=endpoint,
                       config=BotoConfig(retries={"max_attempts": 1}))


def logs_client(endpoint, sess):
    return sess.client("logs", endpoint_url=endpoint,
                       config=BotoConfig(retries={"max_attempts": 1}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="http://localhost:4566")
    args = parser.parse_args()
    ep = args.endpoint

    sess = session(ep)
    sqs  = sqs_client(ep, sess)
    lam  = lambda_client(ep, sess)
    cwl  = logs_client(ep, sess)

    print(f"Seeding resources for account {ACCOUNT} at {ep}\n")

    # ── SQS ─────────────────────────────────────────────────────────────────
    queues = [
        ("acct2-orders",      "Standard", False),
        ("acct2-payments",    "Standard", False),
        ("acct2-events.fifo", "FIFO",     True),
    ]
    print("SQS queues:")
    for name, qtype, fifo in queues:
        attrs = {"FifoQueue": "true"} if fifo else {}
        try:
            r = sqs.create_queue(QueueName=name, Attributes=attrs)
            print(f"  ✓ {name}  ({qtype})  {r['QueueUrl']}")
        except Exception as e:
            print(f"  ✗ {name}: {e}")

    # ── Lambda ───────────────────────────────────────────────────────────────
    role = f"arn:aws:iam::{ACCOUNT}:role/lambda-exec"
    functions = [
        ("acct2-api-handler",   "python3.12", "handler.handler"),
        ("acct2-event-consumer","python3.12", "handler.handler"),
    ]
    print("\nLambda functions:")
    zip_bytes = make_zip(LAMBDA_CODE)
    for fname, runtime, handler in functions:
        try:
            lam.create_function(
                FunctionName=fname,
                Runtime=runtime,
                Role=role,
                Handler=handler,
                Code={"ZipFile": zip_bytes},
                Description=f"Account-2 test function: {fname}",
                Timeout=30,
                MemorySize=256,
            )
            print(f"  ✓ {fname}")
        except lam.exceptions.ResourceConflictException:
            print(f"  ~ {fname} already exists")
        except Exception as e:
            print(f"  ✗ {fname}: {e}")

    # ── CloudWatch Logs ──────────────────────────────────────────────────────
    log_groups = [
        f"/aws/lambda/acct2-api-handler",
        f"/aws/lambda/acct2-event-consumer",
        f"/app/acct2/service",
    ]
    print("\nCloudWatch log groups:")
    for lg in log_groups:
        try:
            cwl.create_log_group(logGroupName=lg)
            print(f"  ✓ {lg}")
        except cwl.exceptions.ResourceAlreadyExistsException:
            print(f"  ~ {lg} already exists")
        except Exception as e:
            print(f"  ✗ {lg}: {e}")

    # Seed a few log events so streams are visible
    try:
        import time
        cwl.create_log_stream(logGroupName="/app/acct2/service", logStreamName="main")
        cwl.put_log_events(
            logGroupName="/app/acct2/service",
            logStreamName="main",
            logEvents=[
                {"timestamp": int(time.time() * 1000), "message": "INFO  account-2 service started"},
                {"timestamp": int(time.time() * 1000) + 1, "message": "INFO  listening on port 8080"},
            ],
        )
        print("  ✓ seeded log events in /app/acct2/service/main")
    except Exception as e:
        print(f"  ✗ log events: {e}")

    print(f"""
Done. Start (or restart) jaiscloud-aws with:

  JAISCLOUD_EXTRA_ACCOUNTS={ACCOUNT} ./jaiscloud-aws start

Then open the UI and switch to account {ACCOUNT} in the top-bar dropdown.
""")


if __name__ == "__main__":
    main()
