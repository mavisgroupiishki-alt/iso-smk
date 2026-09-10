FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# libarchive-tools provides bsdtar, which is used for RAR extraction.  Russian
# Tesseract data keeps the local OCR fallback available for scans in archives.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libarchive-tools tesseract-ocr tesseract-ocr-rus \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

CMD ["python", "server.py"]
