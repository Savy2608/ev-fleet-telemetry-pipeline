# AWS Console Setup Guide

Click-by-click setup for every AWS piece of the pipeline. Use the **same region**
for everything (this guide uses `us-east-1`, N. Virginia).

> **Cost tip:** a `t3.small` costs about $0.02/hour. Stop the instance when you're
> not using it, and see [Cleanup](#8-cleanup-avoid-surprise-charges) when you're done.

---

## 1. Key pair (to SSH into EC2)

1. **EC2 → Network & Security → Key Pairs → Create key pair**
2. Name: `ev-kafka-key`, type **RSA**, format **.pem** → Create (it downloads).
3. On your Mac:
   ```bash
   mkdir -p ~/.ssh && mv ~/Downloads/ev-kafka-key.pem ~/.ssh/
   chmod 400 ~/.ssh/ev-kafka-key.pem
   ```

## 2. Security Group (the EC2 firewall)

1. **EC2 → Security Groups → Create security group**
2. Name: `ev-kafka-sg`, VPC: the default VPC.
3. **Inbound rules** (add two):

   | Type       | Port | Source  | Why                                   |
   |------------|------|---------|---------------------------------------|
   | SSH        | 22   | My IP   | So you can log in from your Mac       |
   | Custom TCP | 9092 | My IP   | So producer/consumer reach Kafka      |

4. Leave outbound as "All traffic" → Create.

> Don't use `0.0.0.0/0` for port 9092: this Kafka has no authentication, so anyone
> on the internet could read or write your topic. If your home IP changes (new Wi-Fi,
> VPN), edit the rule and pick **My IP** again. Port 2181 (ZooKeeper) stays closed;
> only Kafka talks to it, on the same machine.

## 3. Launch the EC2 instance

1. **EC2 → Instances → Launch instances**
2. Name: `ev-kafka-broker`
3. AMI: **Ubuntu Server 24.04 LTS** (64-bit x86)
4. Instance type: **t3.small** (2 GB RAM). `t2.micro`/`t3.micro` (free tier) works
   too thanks to the small heap set in the setup script, but is less stable.
5. Key pair: `ev-kafka-key`
6. Network settings → **Edit** → Auto-assign public IP: **Enable** →
   Select existing security group: `ev-kafka-sg`
7. Storage: 16 GB gp3 → **Launch instance**
8. Copy the **Public IPv4 address** from the instance details page.

Then on your Mac:

```bash
scp -i ~/.ssh/ev-kafka-key.pem scripts/ec2_setup_commands.sh ubuntu@<EC2_PUBLIC_IP>:~
ssh -i ~/.ssh/ev-kafka-key.pem ubuntu@<EC2_PUBLIC_IP>
bash ec2_setup_commands.sh
```

> Optional: **EC2 → Elastic IPs → Allocate → Associate** with the instance so the
> public IP doesn't change every time you stop/start it.

## 4. S3 bucket

1. **S3 → Create bucket**
2. Name must be globally unique, e.g. `ev-fleet-telemetry-<yourname>-2026`
3. Region: same as EC2. Keep **Block all public access** ON. → Create.
4. Inside the bucket, create a folder `athena-results/` (Athena writes query output here).

Data will land under `s3://<bucket>/raw/ev_telemetry/year=…/month=…/day=…/hour=…/`.

## 5. IAM: credentials for your Mac

The consumer on your Mac needs permission to write to the bucket.

1. **IAM → Users → Create user** → name `ev-pipeline-local` (no console access).
2. **Attach policies directly → Create policy → JSON**, paste (replace the bucket name):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": ["s3:ListBucket"],
         "Resource": "arn:aws:s3:::ev-fleet-telemetry-<yourname>-2026"
       },
       {
         "Effect": "Allow",
         "Action": ["s3:PutObject", "s3:GetObject"],
         "Resource": "arn:aws:s3:::ev-fleet-telemetry-<yourname>-2026/*"
       }
     ]
   }
   ```
   Name it `ev-pipeline-s3-write`, then attach it to the user.
3. Open the user → **Security credentials → Create access key → Command Line Interface**.
4. On your Mac (never put these keys in code or in git):
   ```bash
   brew install awscli
   aws configure          # paste the key, secret, region us-east-1, output json
   aws s3 ls s3://ev-fleet-telemetry-<yourname>-2026   # should succeed (empty output is fine)
   ```

## 6. Glue Crawler → Data Catalog table

Run the producer and consumer for a few minutes first so the crawler has files to read.

1. **AWS Glue → Data Catalog → Databases → Add database** → name `ev_fleet_db`.
2. **Glue → Crawlers → Create crawler**
   - Name: `ev-telemetry-crawler`
   - Data source: **S3**, path `s3://<bucket>/raw/ev_telemetry/`
     (point at the folder, **not** the bucket root, or it will also crawl `athena-results/`)
   - IAM role: **Create new IAM role** → `AWSGlueServiceRole-ev-telemetry`
     (the wizard grants it read access to that S3 path)
   - Target database: `ev_fleet_db`
   - Schedule: **On demand** (or **Hourly** so new `hour=` partitions get added automatically)
3. **Run crawler**. After ~1 minute: **Tables** shows `ev_telemetry` with columns
   `event_id, vehicle_id, timestamp, battery_level, battery_temp_c, speed_mph,
   charging_status, station_id, location (struct)` plus partition keys
   `year, month, day, hour`.

> New partitions (a new hour) are invisible to Athena until the crawler runs again,
> or until you run `MSCK REPAIR TABLE ev_fleet_db.ev_telemetry;` in Athena.

## 7. Athena

1. **Athena → Query editor → Settings → Manage** → Query result location:
   `s3://<bucket>/athena-results/` → Save.
2. Data source `AwsDataCatalog`, database `ev_fleet_db`.
3. Run the queries in [`sql/athena_queries.sql`](../sql/athena_queries.sql):
   - **Query 1**: vehicles with battery temperature above 45 °C
   - **Query 2**: overloaded charging stations (more cars charging than ports)
   - **Query 3**: latest status of every vehicle, lowest battery first

## 8. Cleanup (avoid surprise charges)

1. EC2: **Terminate** `ev-kafka-broker` (and release the Elastic IP if you made one;
   an unattached Elastic IP is billed).
2. S3: **Empty** then **Delete** the bucket.
3. Glue: delete the crawler, table and database.
4. IAM: delete the `ev-pipeline-local` user's access key (or the whole user).
