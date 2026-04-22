FROM python:3-alpine

RUN pip3 install pika

COPY common ./common
COPY sum .

ENV PYTHONHASHSEED=0

ENTRYPOINT ["python3", "main.py"]
