FROM python:3.11-alpine

WORKDIR /usr/src/app

COPY requirements.txt .
RUN pip install --require-hashes -r requirements.txt

COPY . .

CMD ["python", "./server.py"]
