#!/usr/bin/env bash
# =============================================================================
# Kafka + ZooKeeper setup on an Ubuntu EC2 instance (Ubuntu 22.04 / 24.04 / 26.04).
#
# Run it ON THE EC2 INSTANCE, either all at once:
#     scp -i ~/.ssh/ev-kafka-key.pem scripts/ec2_setup_commands.sh ubuntu@<EC2_PUBLIC_IP>:~
#     ssh -i ~/.ssh/ev-kafka-key.pem ubuntu@<EC2_PUBLIC_IP>
#     bash ec2_setup_commands.sh
# or copy-paste each numbered step to see what it does.
#
# Kafka 3.9.x is the last release line that still supports ZooKeeper.
# (Kafka 4.x is KRaft-only; see the note at the bottom.)
# =============================================================================
set -euo pipefail

KAFKA_VERSION="3.9.1"
SCALA_VERSION="2.13"
KAFKA_DIR="$HOME/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TOPIC="ev-telemetry"

# -----------------------------------------------------------------------------
# 1. Install Java (Kafka runs on the JVM)
# -----------------------------------------------------------------------------
sudo apt-get update -y
sudo apt-get install -y openjdk-17-jre-headless wget curl
java -version

# -----------------------------------------------------------------------------
# 2. Download and extract Kafka
# -----------------------------------------------------------------------------
cd "$HOME"
if [ ! -d "$KAFKA_DIR" ]; then
  wget "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
fi
cd "$KAFKA_DIR"

# -----------------------------------------------------------------------------
# 3. Small-instance memory settings
#    Kafka defaults to a 1 GB heap, which crashes a t2.micro/t3.micro (1 GB RAM).
#    Saved to ~/.bashrc so it also applies after you log in again.
# -----------------------------------------------------------------------------
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"
grep -q KAFKA_HEAP_OPTS ~/.bashrc || echo 'export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"' >> ~/.bashrc

# -----------------------------------------------------------------------------
# 4. Edit config/server.properties
#    - listeners: what the broker binds to on the instance (all interfaces)
#    - advertised.listeners: the address the broker hands back to clients.
#      It MUST be the EC2 *public* IP, otherwise your Mac connects once and
#      then gets redirected to a private 172.31.x.x address it can't reach.
#    - log.dirs: keep data out of /tmp so it survives a reboot.
# -----------------------------------------------------------------------------
# Ask the EC2 metadata service (IMDSv2) for this instance's public IPv4.
IMDS_TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
PUBLIC_IP=$(curl -s -H "X-aws-ec2-metadata-token: $IMDS_TOKEN" \
  "http://169.254.169.254/latest/meta-data/public-ipv4")
echo "EC2 public IP: $PUBLIC_IP"

CONFIG="config/server.properties"
[ -f "$CONFIG.orig" ] || cp "$CONFIG" "$CONFIG.orig"   # keep a backup of the original file
sed -i -E "s|^#?listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092|" "$CONFIG"
sed -i -E "s|^#?advertised.listeners=.*|advertised.listeners=PLAINTEXT://${PUBLIC_IP}:9092|" "$CONFIG"
sed -i -E "s|^log.dirs=.*|log.dirs=$HOME/kafka-data/kafka-logs|" "$CONFIG"
sed -i -E "s|^dataDir=.*|dataDir=$HOME/kafka-data/zookeeper|" config/zookeeper.properties

echo "--- Effective settings ---"
grep -E "^(listeners|advertised.listeners|log.dirs)=" "$CONFIG"
grep -E "^dataDir=" config/zookeeper.properties

# -----------------------------------------------------------------------------
# 5. Start ZooKeeper, then Kafka (both in the background with -daemon)
#    If they're already running (re-run of this script), restart them so the
#    new config is picked up.
# -----------------------------------------------------------------------------
if pgrep -f kafka.Kafka >/dev/null; then bin/kafka-server-stop.sh; sleep 10; fi
if pgrep -f QuorumPeerMain >/dev/null; then bin/zookeeper-server-stop.sh; sleep 5; fi

bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
echo "Waiting for ZooKeeper..."; sleep 10

bin/kafka-server-start.sh -daemon config/server.properties
echo "Waiting for Kafka..."; sleep 15

# -----------------------------------------------------------------------------
# 6. Create the topic (3 partitions; 1 replica because there's only 1 broker)
# -----------------------------------------------------------------------------
bin/kafka-topics.sh --create --if-not-exists \
  --topic "$TOPIC" \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

bin/kafka-topics.sh --describe --topic "$TOPIC" --bootstrap-server localhost:9092

cat <<EOF

=============================================================================
 Kafka is running.  Put this in your Mac's .env file:
     KAFKA_BOOTSTRAP_SERVERS=${PUBLIC_IP}:9092
=============================================================================

Useful commands (run from $KAFKA_DIR):

  # Watch messages arriving (Ctrl+C to stop)
  bin/kafka-console-consumer.sh --topic $TOPIC --bootstrap-server localhost:9092

  # Check the S3 sink's consumer group lag
  bin/kafka-consumer-groups.sh --describe --group ev-telemetry-s3-sink --bootstrap-server localhost:9092

  # Is it running?  (should list Kafka and QuorumPeerMain)
  jps

  # Logs if something fails
  tail -n 50 logs/server.log
  tail -n 50 logs/zookeeper.out

  # Stop everything (Kafka first, then ZooKeeper)
  bin/kafka-server-stop.sh
  bin/zookeeper-server-stop.sh

EOF

# -----------------------------------------------------------------------------
# AFTER STOPPING/STARTING THE INSTANCE
#   EC2 gives you a NEW public IP on every start (unless you attach an
#   Elastic IP). Re-run this script: it skips the download, rewrites
#   advertised.listeners with the new IP and starts the services again.
#   Then update KAFKA_BOOTSTRAP_SERVERS in your .env.
#
# NOTE ON KAFKA 4.x (KRaft, no ZooKeeper)
#   With Kafka 4.x, skip ZooKeeper and instead run once:
#     KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
#     bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
#   then start only: bin/kafka-server-start.sh -daemon config/server.properties
# -----------------------------------------------------------------------------
