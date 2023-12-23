FROM python:3 AS credits-base

WORKDIR /app

COPY requirements.txt ./

RUN pip install -r requirements.txt