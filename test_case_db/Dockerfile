# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /ohlcv

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD [ "python3", "data_process.py"]
