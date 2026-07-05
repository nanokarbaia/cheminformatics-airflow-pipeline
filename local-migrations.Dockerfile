FROM python:3.11-slim

RUN pip install --no-cache-dir yoyo-migrations psycopg2-binary

WORKDIR /migrations

COPY migrations/ .

CMD ["yoyo", "apply", "--no-config-file", "--database", "postgresql://postgres:123456@local-db:5432/postgres", "/migrations"]
