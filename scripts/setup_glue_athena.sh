#!/usr/bin/env bash
# Catalog the raw EV telemetry data with AWS Glue and run test queries on Amazon Athena.
#
# Usage:
#   ./scripts/setup_glue_athena.sh
#
# Optional overrides (environment variables):
#   AWS_REGION  (default: us-east-2)
#   BUCKET      (default: ev-fleet-telemetry-vyngo-2026)
#
# The script is idempotent: it skips resources that already exist.

set -euo pipefail

REGION="${AWS_REGION:-us-east-2}"
BUCKET="${BUCKET:-ev-fleet-telemetry-vyngo-2026}"
DATABASE="ev_fleet_db"
TABLE="ev_telemetry"
CRAWLER="ev-telemetry-crawler"
ROLE_NAME="AWSGlueServiceRole-ev-telemetry"
S3_TARGET="s3://${BUCKET}/raw/ev_telemetry/"
# Keep Athena results OUTSIDE the crawler path so they are never crawled as data.
ATHENA_OUTPUT="s3://${BUCKET}/athena-results/"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
echo "==> Region: ${REGION} | Bucket: ${BUCKET}"

# ---------------------------------------------------------------------------
# 1. IAM role for the Glue crawler
# ---------------------------------------------------------------------------
if aws iam get-role --role-name "${ROLE_NAME}" >/dev/null 2>&1; then
  echo "==> IAM role ${ROLE_NAME} already exists"
else
  echo "==> Creating IAM role ${ROLE_NAME}"
  aws iam create-role --role-name "${ROLE_NAME}" \
    --assume-role-policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},
        "Action": "sts:AssumeRole"
      }]
    }' >/dev/null

  aws iam attach-role-policy --role-name "${ROLE_NAME}" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

  # Least privilege: read-only access to the raw zone of this bucket only.
  aws iam put-role-policy --role-name "${ROLE_NAME}" \
    --policy-name s3-read-ev-telemetry-raw \
    --policy-document "{
      \"Version\": \"2012-10-17\",
      \"Statement\": [
        {\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\"],
         \"Resource\": \"arn:aws:s3:::${BUCKET}/raw/*\"},
        {\"Effect\": \"Allow\", \"Action\": [\"s3:ListBucket\"],
         \"Resource\": \"arn:aws:s3:::${BUCKET}\"}
      ]
    }"

  echo "==> Waiting for the IAM role to propagate"
  sleep 15
fi

# ---------------------------------------------------------------------------
# 2. Glue database
# ---------------------------------------------------------------------------
if aws glue get-database --name "${DATABASE}" --region "${REGION}" >/dev/null 2>&1; then
  echo "==> Glue database ${DATABASE} already exists"
else
  echo "==> Creating Glue database ${DATABASE}"
  aws glue create-database --region "${REGION}" \
    --database-input "{\"Name\": \"${DATABASE}\", \"Description\": \"EV fleet telemetry data lake\"}"
fi

# ---------------------------------------------------------------------------
# 3. Glue crawler
# ---------------------------------------------------------------------------
if aws glue get-crawler --name "${CRAWLER}" --region "${REGION}" >/dev/null 2>&1; then
  echo "==> Glue crawler ${CRAWLER} already exists"
else
  echo "==> Creating Glue crawler ${CRAWLER}"
  aws glue create-crawler --region "${REGION}" \
    --name "${CRAWLER}" \
    --role "${ROLE_ARN}" \
    --database-name "${DATABASE}" \
    --targets "{\"S3Targets\": [{\"Path\": \"${S3_TARGET}\"}]}" \
    --schema-change-policy '{"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"}'
fi

echo "==> Starting crawler"
aws glue start-crawler --name "${CRAWLER}" --region "${REGION}"

sleep 10
while true; do
  STATE=$(aws glue get-crawler --name "${CRAWLER}" --region "${REGION}" \
    --query 'Crawler.State' --output text)
  echo "    crawler state: ${STATE}"
  [[ "${STATE}" == "READY" ]] && break
  sleep 15
done

LAST_STATUS=$(aws glue get-crawler --name "${CRAWLER}" --region "${REGION}" \
  --query 'Crawler.LastCrawl.Status' --output text)
echo "==> Last crawl status: ${LAST_STATUS}"
if [[ "${LAST_STATUS}" != "SUCCEEDED" ]]; then
  aws glue get-crawler --name "${CRAWLER}" --region "${REGION}" \
    --query 'Crawler.LastCrawl.ErrorMessage' --output text >&2
  exit 1
fi

echo "==> Tables in ${DATABASE}:"
aws glue get-tables --database-name "${DATABASE}" --region "${REGION}" \
  --query 'TableList[].Name' --output text

echo "==> Schema of ${TABLE}:"
aws glue get-table --database-name "${DATABASE}" --name "${TABLE}" --region "${REGION}" \
  --query 'Table.StorageDescriptor.Columns[].[Name, Type]' --output table
echo "==> Partition keys:"
aws glue get-table --database-name "${DATABASE}" --name "${TABLE}" --region "${REGION}" \
  --query 'Table.PartitionKeys[].[Name, Type]' --output table

# ---------------------------------------------------------------------------
# 4. Athena test queries
# ---------------------------------------------------------------------------
run_athena() {
  local sql="$1"
  local qid state
  echo
  echo "==> Athena: ${sql}"
  qid=$(aws athena start-query-execution --region "${REGION}" \
    --query-string "${sql}" \
    --query-execution-context Database="${DATABASE}" \
    --result-configuration OutputLocation="${ATHENA_OUTPUT}" \
    --query 'QueryExecutionId' --output text)

  while true; do
    state=$(aws athena get-query-execution --region "${REGION}" \
      --query-execution-id "${qid}" \
      --query 'QueryExecution.Status.State' --output text)
    case "${state}" in
      SUCCEEDED) break ;;
      FAILED|CANCELLED)
        echo "Query ${state}:" >&2
        aws athena get-query-execution --region "${REGION}" \
          --query-execution-id "${qid}" \
          --query 'QueryExecution.Status.StateChangeReason' --output text >&2
        return 1 ;;
    esac
    sleep 2
  done

  aws athena get-query-results --region "${REGION}" \
    --query-execution-id "${qid}" --output table
}

# Partition columns from Hive-style paths are strings, so compare with quoted values.
run_athena "SELECT * FROM ${TABLE} WHERE year = '2026' AND month = '11' AND day = '17' LIMIT 10"

run_athena "SELECT year, month, day, hour, COUNT(*) AS records
            FROM ${TABLE}
            GROUP BY year, month, day, hour
            ORDER BY year, month, day, hour"

# Per-vehicle battery / speed stats.
run_athena "SELECT vehicle_id,
                   COUNT(*)                      AS records,
                   ROUND(AVG(battery_level), 1)  AS avg_battery_pct,
                   MIN(battery_level)            AS min_battery_pct,
                   ROUND(AVG(battery_temp_c), 1) AS avg_battery_temp_c,
                   ROUND(AVG(speed_mph), 1)      AS avg_speed_mph,
                   MAX(speed_mph)                AS max_speed_mph
            FROM ${TABLE}
            WHERE year = '2026' AND month = '11' AND day = '17'
            GROUP BY vehicle_id
            ORDER BY vehicle_id"

# Charging status breakdown (nested struct fields use dot notation: location.lat).
run_athena "SELECT charging_status,
                   COUNT(*)                     AS records,
                   ROUND(AVG(battery_level), 1) AS avg_battery_pct
            FROM ${TABLE}
            GROUP BY charging_status
            ORDER BY records DESC"

echo
echo "==> Done."
