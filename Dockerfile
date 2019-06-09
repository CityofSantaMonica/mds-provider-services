FROM python:3.7

WORKDIR /usr/src/mds

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

EXPOSE 8888