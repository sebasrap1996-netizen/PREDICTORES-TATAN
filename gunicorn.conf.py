import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
workers = 1          # Single worker — WS connections live in-process
threads = 4          # Handle concurrent HTTP + SSE
timeout = 120        # SSE streams need longer timeout
keepalive = 5
loglevel = "info"
accesslog = "-"
errorlog = "-"
preload_app = True
