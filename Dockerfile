FROM python:3.12-slim-bookworm

RUN useradd -m -u 1001 runner

WORKDIR /app
COPY codhoot-python-service .

ENV PORT=8081
EXPOSE 8081

CMD ["./codhoot-python-service"]
