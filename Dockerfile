FROM python:3.13-alpine

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --require-hashes -r requirements.txt

COPY . .

ENV \
  DOORMAN_CACHE_PATH=/cache \
  DOORMAN_COMMAND_SOCKET=/run/doorman.sock

CMD ["./server.py"]
VOLUME ["/cache"]
