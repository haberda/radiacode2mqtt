FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Bleak uses the host BlueZ service over D-Bus; USB needs only libusb.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libusb-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
# Fail visibly if a supported architecture lacks wheels; never retain a compiler.
RUN pip install --no-cache-dir --no-compile --only-binary=:all: -r requirements.txt

COPY app/ /app/
COPY run.sh /run.sh
RUN chmod 755 /run.sh

CMD ["/run.sh"]
