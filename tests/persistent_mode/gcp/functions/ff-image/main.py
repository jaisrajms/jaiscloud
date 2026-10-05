"""Functions-Framework handlers for the Cloud Functions native-executor e2e.

Both an HTTP-signature handler and a CloudEvent-signature handler are exported;
the deployed function's entryPoint selects which one the framework runs via
FUNCTION_TARGET.
"""

import functions_framework


@functions_framework.http
def hello(request):
    """HTTP handler: echo the request method and body back as JSON."""
    return {
        "hello": "world",
        "method": request.method,
        "body": request.get_data(as_text=True),
    }


@functions_framework.cloud_event
def hello_event(cloud_event):
    """CloudEvent handler: echo the event type and data back as JSON."""
    return {
        "hello": "event",
        "type": cloud_event["type"],
        "source": cloud_event["source"],
    }
