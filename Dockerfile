FROM python:3.11-alpine

WORKDIR /usr/src/app

COPY requirements.txt .
RUN ENABLE_DJB_HASH_CEXT=0 pip install --require-hashes -r requirements.txt

COPY . .

CMD ["python", "./server.py"]
