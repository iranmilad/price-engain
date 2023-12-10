FROM python:3.8

RUN python -m venv venv

ENV VIRTUAL_ENV=/app/venv

COPY . /app

RUN pip install -r requirements.txt

RUN for f in 1d.py 1h.py 1m.py 1min.py 1w.py 2h.py 4h.py 5min.py 8h.py 10min.py 15min.py 30min.py; do  CMD ["python", "$f"] done

HEALTHCHECK --interval=5m --timeout=3s --start-period=30s CMD curl --fail http://localhost:8000