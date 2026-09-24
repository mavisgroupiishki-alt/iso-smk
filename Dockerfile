FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# libarchive-tools provides bsdtar for RAR extraction; antiword reads legacy
# Word .doc files. Russian Tesseract data keeps local OCR available for scans.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libarchive-tools antiword tesseract-ocr tesseract-ocr-rus \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

CMD ["python", "server.py"]
