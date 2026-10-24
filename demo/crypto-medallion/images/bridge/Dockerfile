# crypto-medallion bridge image (design §5.2). Built with the demo root as the
# build context so the replay capture (feed/replay.jsonl.gz) ships in the image.
FROM python:3.12-slim

WORKDIR /app
COPY images/bridge/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY images/bridge/bridge.py /app/bridge.py
COPY feed/replay.jsonl.gz /app/replay.jsonl.gz

# The bridge is long-running; default to the live feed with replay fallback.
ENV SOURCE=auto
ENTRYPOINT ["python3", "/app/bridge.py"]
