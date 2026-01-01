#!/usr/bin/env bash
set -euo pipefail

# ===== Inputs =====
ACCOUNT_P_ID="976053906881"
PROVIDER_ARN="arn:aws:iam::976053906881:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/E10CAD8EC223A0EDA7E58FCD6FC754DC"

ACCOUNT_D_CLUSTER_OIDC_ISSUER="https://oidc.eks.us-west-2.amazonaws.com/id/E10CAD8EC223A0EDA7E58FCD6FC754DC"
K8S_NAMESPACE="foxglove"
K8S_SERVICEACCOUNT="data-archiver"

ROLE_NAME="EKS-IRSA-S3ReadBags-FullTripClips"

BAGS_BUCKET="coco-gg-bags-prod"
TRIP_CLIPS_BUCKET="coco-trip-clips-976053906881-us-west-2"
# ===== /Inputs =====

OIDC_HOSTPATH="${ACCOUNT_D_CLUSTER_OIDC_ISSUER#https://}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_P_ID}:role/${ROLE_NAME}"

TRUST_JSON="$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "${PROVIDER_ARN}" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "${OIDC_HOSTPATH}:aud": "sts.amazonaws.com",
        "${OIDC_HOSTPATH}:sub": "system:serviceaccount:${K8S_NAMESPACE}:${K8S_SERVICEACCOUNT}"
      }
    }
  }]
}
EOF
)"

POLICY_JSON="$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadBags",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::${BAGS_BUCKET}"
    },
    {
      "Sid": "ReadBagsObjects",
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::${BAGS_BUCKET}/*"
    },
    {
      "Sid": "TripClipsList",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::${TRIP_CLIPS_BUCKET}"
    },
    {
      "Sid": "TripClipsObjectsAll",
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": "arn:aws:s3:::${TRIP_CLIPS_BUCKET}/*"
    }
  ]
}
EOF
)"

if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "${ROLE_NAME}" --policy-document "${TRUST_JSON}" >/dev/null
else
  aws iam create-role \
    --role-name "${ROLE_NAME}" \
    --assume-role-policy-document "${TRUST_JSON}" \
    --description "IRSA role for ${K8S_NAMESPACE}/${K8S_SERVICEACCOUNT} to access S3 buckets in Account P" \
    >/dev/null
fi

aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name "inline-s3-access" \
  --policy-document "${POLICY_JSON}" \
  >/dev/null

echo "Role ready: ${ROLE_ARN}"
echo "Annotate k8s ServiceAccount with:"
echo "  eks.amazonaws.com/role-arn: ${ROLE_ARN}"
