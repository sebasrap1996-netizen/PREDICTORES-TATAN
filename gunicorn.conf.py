import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
workers = 1          # Single worker — WS connections viven en el mismo proceso
threads = 4          # HTTP + SSE concurrentes
timeout = 0          # CRÍTICO: 0 = sin timeout — Gunicorn nunca mata workers por tiempo
                     # El timeout=120 anterior mataba workers con SSE streams activos,
                     # lo que cerraba todos los WebSockets cada 2 minutos
keepalive = 5
loglevel = "info"
accesslog = "-"
errorlog = "-"
preload_app = True

def post_fork(server, worker):
    """
    Se ejecuta DENTRO del worker después del fork.
    Con preload_app=True los hilos del master no se heredan —
    hay que relanzarlos aquí en el worker.
    """
    os.environ["GUNICORN_WORKER_STARTED"] = "1"
    try:
        from app import _autostart, _watchdog, _periodic_save
        import threading
        threading.Thread(target=_autostart,     daemon=True).start()
        threading.Thread(target=_watchdog,      daemon=True).start()
        threading.Thread(target=_periodic_save, daemon=True).start()
        server.log.info("post_fork: autostart + watchdog + periodic_save lanzados en worker pid=%d",
                        worker.pid)
    except Exception as e:
        server.log.error("post_fork ERROR: %s", e)
