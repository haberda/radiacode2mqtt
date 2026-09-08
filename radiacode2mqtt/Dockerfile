FROM python:3.11-slim-bookworm AS builder

# bluepy still provides the application's BLE preflight scan. Compile its
# helper here so compilers and development headers stay out of the runtime.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
RUN /opt/venv/bin/pip install --no-cache-dir \
    paho-mqtt \
    radiacode \
    aiohttp \
    bluepy

FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Runtime libraries, Bluetooth utilities, and helper-process recovery tools.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    libusb-1.0-0 \
    bluez \
    dbus \
    procps \
    psmisc \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

WORKDIR /app

COPY app/ /app/
COPY run.sh /run.sh
RUN chmod 755 /run.sh

CMD ["/run.sh"]
