# A minimal Functions-Framework image for the Cloud Functions native-executor
# e2e gates. The emulator mounts the deployed source archive at /workspace and
# starts the framework with the FUNCTION_TARGET / FUNCTION_SIGNATURE_TYPE the
# gcf profile injects; the framework CLI reads both from the environment.
FROM python:3.12-slim

RUN pip install --no-cache-dir "functions-framework==3.*"

WORKDIR /workspace

ENV PORT=8080

# No --target: the Functions Framework CLI reads FUNCTION_TARGET (set per
# function by the emulator) and defaults --source to the working directory.
CMD ["functions-framework"]
