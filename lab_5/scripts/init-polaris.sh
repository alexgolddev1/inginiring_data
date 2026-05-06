#!/usr/bin/env bash

set -euo pipefail

POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_command curl
require_command jq

ACCESS_TOKEN="$(
  curl -fsS -X POST \
    "${POLARIS_URL}/api/catalog/v1/oauth/tokens" \
    -d 'grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL' \
    | jq -r '.access_token'
)"

if [[ -z "${ACCESS_TOKEN}" || "${ACCESS_TOKEN}" == "null" ]]; then
  echo "Failed to obtain Polaris access token" >&2
  exit 1
fi

curl -fsS -X POST \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/catalogs" \
  --json '{
    "name": "polariscatalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://warehouse",
      "s3.endpoint": "http://minio:9000",
      "s3.path-style-access": "true",
      "s3.access-key-id": "admin",
      "s3.secret-access-key": "password",
      "s3.region": "dummy-region"
    },
    "storageConfigInfo": {
      "roleArn": "arn:aws:iam::000000000000:role/minio-polaris-role",
      "storageType": "S3",
      "allowedLocations": [
        "s3://warehouse/*"
      ]
    }
  }' >/dev/null || true

curl -fsS -X PUT \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants" \
  --json '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' >/dev/null

curl -fsS -X POST \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/principal-roles" \
  --json '{"principalRole":{"name":"data_engineer"}}' >/dev/null || true

curl -fsS -X PUT \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog" \
  --json '{"catalogRole":{"name":"catalog_admin"}}' >/dev/null

curl -fsS -X PUT \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/principals/root/principal-roles" \
  --json '{"principalRole":{"name":"data_engineer"}}' >/dev/null

curl -fsS \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/catalogs" | jq

curl -fsS \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  "${POLARIS_URL}/api/management/v1/principals/root/principal-roles" | jq
