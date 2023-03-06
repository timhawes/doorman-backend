FROM python:3.11

WORKDIR /usr/src/app

COPY Pipfile Pipfile.lock ./
RUN apt-get update \
    && apt-get install -y --no-install-recommends pipenv \
    && rm -r /var/lib/apt/lists/* \
    && pipenv install --deploy --system

COPY . .

CMD ["python", "./server.py"]
