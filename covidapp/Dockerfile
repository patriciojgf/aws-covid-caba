FROM python:3.7-slim

RUN apt-get update && apt-get -y install libpq-dev gcc python3-dev musl-dev 

RUN python3 -m pip install -U pip setuptools

RUN python3 -m pip install xlrd

RUN python3 -m pip install boto3

COPY . .

RUN pip3 install -r requirements.txt

CMD ["python3", "init_run.py"]