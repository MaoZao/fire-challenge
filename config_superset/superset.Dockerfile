FROM apache/superset:latest

USER root
RUN pip install sqlalchemy-trino psycopg2-binary


USER superset