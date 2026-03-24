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

def post_fork(server, worker):
    """
    Se ejecuta en el proceso WORKER (no en el master) después del fork.
    Aquí es donde deben arrancar los hilos de WebSocket, porque con
    preload_app=True el código del módulo se carga en el master y los
    hilos daemon lanzados allí NO se heredan por los workers.
    """
    import os as _os
    _os.environ["GUNICORN_WORKER_STARTED"] = "1"

    # Importar la app ya cargada (preload_app la cargó en el master)
    try:
        from app import _autostart, _watchdog, _periodic_save
        import threading
        threading.Thread(target=_autostart, daemon=True).start()
        threading.Thread(target=_watchdog,  daemon=True).start()
        threading.Thread(target=_periodic_save, daemon=True).start()
        server.log.info("post_fork: WebSocket autostart + watchdog lanzados en worker pid=%d", worker.pid)
    except Exception as e:
        server.log.error("post_fork ERROR: %s", e)
