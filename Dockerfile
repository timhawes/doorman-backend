FROM python:3.14-alpine
COPY --from=ghcr.io/astral-sh/uv:0.11.23 /uv /uvx /bin/

COPY . /app
WORKDIR /app
RUN uv sync --locked
ENV PATH="/app/.venv/bin:$PATH"

ENV \
  DOORMAN_CACHE_PATH=/cache \
  DOORMAN_COMMAND_SOCKET=/run/doorman.sock

CMD ["./server.py"]
VOLUME ["/cache"]
