FROM python:3.12-slim
WORKDIR /app
COPY publisher.py /app/publisher.py
ENV PORT=8080
ENTRYPOINT ["python3", "/app/publisher.py"]
