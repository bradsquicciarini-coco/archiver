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

## Tar

Dedupe

```sql
copy (
with tmp as (
    select
      * exclude (_col5),
      _col5::json as user_metadata,
      row_number() over (partition by key order by record_timestamp desc) as rn
    from './data/trip_clips.csv' qualify rn = 1
  )

  select * exclude (rn) from tmp
) to './data/trip_clips.parquet';
```

```sql
select
  user_metadata->>'location__city' as city,
  user_metadata->>'reference_id' as pilot_assignment_id,
  user_metadata->>'vehicle__camera_version' as pilot_assignment_id,
  key,
  size,
  user_metadata
from './trip_clips_deduped.parquet'
limit 10;
```
