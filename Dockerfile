FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
ARG DEEPLOB_INSTALL=base
COPY requirements.txt requirements-deeplob-recorder.txt requirements-deeplob.txt pyproject.toml ./
COPY dhan_engine ./dhan_engine
RUN pip install --no-cache-dir -r requirements.txt \
    && if [ "$DEEPLOB_INSTALL" = "recorder" ]; then pip install --no-cache-dir -r requirements-deeplob-recorder.txt; fi \
    && if [ "$DEEPLOB_INSTALL" = "inference" ]; then pip install --no-cache-dir -r requirements-deeplob.txt; fi \
    && pip install --no-cache-dir .

CMD ["python", "-m", "dhan_engine.interfaces.cli.run_service"]
