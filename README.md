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
SQS_ENDPOINT_URL=http://localhost:4566 QUEUE_URL=<queue-url> uv run scripts/sqs_worker.py
```

Testing docker locally

```bash
  docker run --rm \
    --add-host=host.docker.internal:host-gateway \
    -e AWS_ACCESS_KEY_ID="" \
    -e AWS_SECRET_ACCESS_KEY="" \
    -e AWS_SESSION_TOKEN="" \
    -e AWS_REGION=us-east-1 \
    -e SQS_ENDPOINT_URL=http://host.docker.internal:4566 \
    -e QUEUE_URL=<queue url> \
    data-archiver-worker \
    --once
```

### k8s

to scale from cli
```bash
kubectl scale deployment data-archiver --replicas=128 -n foxglove
```
