FROM python:3.11-alpine

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --require-hashes -r requirements.txt

COPY . .

ENV \
  CACHE_PATH=/cache \
  COMMAND_SOCKET=/run/doorman.sock

CMD ["./server.py"]
VOLUME ["/cache"]
