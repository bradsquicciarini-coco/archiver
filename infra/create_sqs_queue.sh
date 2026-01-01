#!/usr/bin/env bash
set -euo pipefail

queue_name="${QUEUE_NAME:-data-archiver-queue}"
dlq_name="${DLQ_NAME:-${queue_name}-dlq}"
aws_region="${AWS_REGION:-us-west-2}"
max_receive_count="${MAX_RECEIVE_COUNT:-5}"

endpoint_args=()
if [[ -n "${SQS_ENDPOINT_URL:-}" ]]; then
  endpoint_args+=(--endpoint-url "${SQS_ENDPOINT_URL}")
fi

dlq_url="$(aws "${endpoint_args[@]}" sqs create-queue \
  --queue-name "${dlq_name}" \
  --region "${aws_region}" \
  --output text \
  --query 'QueueUrl')"

dlq_arn="$(aws "${endpoint_args[@]}" sqs get-queue-attributes \
  --queue-url "${dlq_url}" \
  --region "${aws_region}" \
  --attribute-names QueueArn \
  --output text \
  --query 'Attributes.QueueArn')"

redrive_policy="$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":%s}' "${dlq_arn}" "${max_receive_count}")"
escaped_redrive_policy="${redrive_policy//\"/\\\"}"
attributes_json="$(printf '{"RedrivePolicy":"%s"}' "${escaped_redrive_policy}")"

aws "${endpoint_args[@]}" sqs create-queue \
  --queue-name "${queue_name}" \
  --region "${aws_region}" \
  --attributes "${attributes_json}" \
  --output text \
  --query 'QueueUrl'
