# environment variables for postgres are set by ECS service env vars, image won't work without
FROM python:3.9.9-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home/ \
  POETRY_VIRTUALENVS_CREATE=false

RUN apt-get update && apt-get upgrade -yqq
RUN apt-get install git libpq-dev -y 

RUN pip install poetry
COPY pyproject.toml poetry.lock ./
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes
RUN pip install -r requirements.txt

RUN mkdir -p /opt/dagster/dagster_home/

WORKDIR /opt/dagster/dagster_home/

COPY dagster.yaml workspace.yaml ./

EXPOSE 8080