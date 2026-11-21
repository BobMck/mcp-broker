# Build stage: turn uv.lock into a hash-pinned requirements file so the image
# installs exactly the locked versions (uv itself is not copied to the final image).
FROM python:3.11-slim AS lock-export

WORKDIR /build

RUN pip install --no-cache-dir uv==0.11.25
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --extra all --no-emit-project --format requirements-txt \
    -o requirements.txt

FROM python:3.11-slim

WORKDIR /app

# Install locked dependencies; --require-hashes rejects anything not in uv.lock
COPY --from=lock-export /build/requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt

# Copy source and config (needed for setuptools package discovery)
COPY pyproject.toml .
COPY src/ src/

# Install the project itself; dependencies are already installed from the lock
RUN pip install --no-cache-dir --no-deps .

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

RUN useradd -m -u 1000 appuser && mkdir -p /app/data && chown appuser:appuser /app/data
USER appuser

EXPOSE 8002

# NOTE: --forwarded-allow-ips should be restricted to known proxy IPs in production
CMD ["uvicorn", "broker.main:app", "--host", "0.0.0.0", "--port", "8002", "--proxy-headers", "--forwarded-allow-ips", "*"]
