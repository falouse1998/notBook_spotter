# Debian ships chromium and a chromedriver built against that exact build, which
# is what keeps the pair from drifting apart on a rebuild.
FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    CHROME_PATH=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    STATE_DIR=/state \
    TZ=Europe/Berlin

RUN apt-get update && apt-get install -y --no-install-recommends \
        chromium \
        chromium-driver \
        fonts-liberation \
        ca-certificates \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Five Chromes share this; the default 64MB is what --disable-dev-shm-usage
# works around, and the run command gives it a real 1GB instead.
VOLUME ["/state"]

CMD ["python", "main.py"]
