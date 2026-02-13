FROM python:3.11-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps for bluepy + bluetooth + pyusb backend
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    make \
    libglib2.0-dev \
    libffi-dev \
    libssl-dev \
    libusb-1.0-0 \
    bluez \
    dbus \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    paho-mqtt \
    radiacode \
    aiohttp \
    bluepy

COPY app/ /app/
COPY run.sh /run.sh
RUN chmod 755 /run.sh

CMD ["/run.sh"]
