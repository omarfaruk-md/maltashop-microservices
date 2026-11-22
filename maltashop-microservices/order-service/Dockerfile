# ---- build stage: dependencies are installed into a virtualenv here ----
FROM python:3.12-slim AS builder
WORKDIR /build
COPY requirements.txt .
RUN python -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# ---- runtime stage: only the virtualenv and the application code ----
FROM python:3.12-slim
ENV PATH="/opt/venv/bin:$PATH" PYTHONUNBUFFERED=1
RUN useradd --uid 10001 --no-create-home appuser && mkdir /data && chown appuser /data
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY service_base.py handlers.py app.py ./
USER appuser
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
  CMD python -c "import urllib.request as u; u.urlopen('http://localhost:8000/health')"
# one worker: exactly one outbox relay and one consumer per container
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "4", "app:app"]
