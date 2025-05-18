FROM python:3-slim
WORKDIR /programas/ingesta02
RUN pip3 install boto3 mysql-connector-python pandas
COPY . .
CMD [ "python3", "./ingesta02.py" ]
