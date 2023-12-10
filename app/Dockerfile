FROM python:3.9

# Set the working directory
WORKDIR /app

# Create and activate the virtual environment
RUN python -m venv venv
ENV PATH="/app/venv/bin:$PATH"

# Copy the application files and install dependencies
COPY . /app
RUN pip install -r requirements.txt

# Run the Python scripts during the image build
RUN for f in 1d.py 1h.py 1m.py 1min.py 1w.py 2h.py 4h.py 5min.py 8h.py 10min.py 15min.py 30min.py; do python "$f"; done

# Healthcheck configuration
HEALTHCHECK --interval=5m --timeout=3s --start-period=30s CMD curl --fail http://localhost:8000
