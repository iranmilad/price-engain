FROM python:3.8

RUN python -m venv venv

ENV VIRTUAL_ENV=/app/venv

COPY . /app

RUN pip install -r requirements.txt

CMD ["python", "/app/start.py"]

HEALTHCHECK --interval=5m --timeout=3s --start-period=30s CMD curl --fail http://localhost:8000