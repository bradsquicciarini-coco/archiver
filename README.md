# archiver

## SQS worker skeleton

Start localstack:

```bash
docker compose -f docker-compose.localstack.yml up -d
```

```bash
  export AWS_ACCESS_KEY_ID=test
  export AWS_SECRET_ACCESS_KEY=test
  export AWS_REGION=us-east-1
```

Create a queue and enqueue a test message:

```bash
SQS_ENDPOINT_URL=http://localhost:4566 uv run scripts/localstack_sqs_demo.py
```

Run the worker (use the printed queue URL):

```bash
SQS_ENDPOINT_URL=http://localhost:4566 uv run scripts/sqs_worker.py --queue-url <queue-url>
```
