FROM python:3.9

RUN pip install great_expectations sqlalchemy pyodbc

COPY config.yaml /app/config.yaml

WORKDIR /app

CMD ["great_expectations", "init"]