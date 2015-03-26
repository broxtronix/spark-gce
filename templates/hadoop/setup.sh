#!/bin/bash

EPHEMERAL_HDFS=$HOME/ephemeral-hdfs

# Set hdfs url to make it easier
export PUBLIC_DNS=`wget -q -O - http://icanhazip.com/`

HDFS_URL="hdfs://$PUBLIC_DNS:9000"
echo "export HDFS_URL=$HDFS_URL" >> ~/.bashrc

$HOME/spark/bin/copy-dir $EPHEMERAL_HDFS/conf

NAMENODE_DIR=/mnt/ephemeral-hdfs/dfs/name

if [ -f "$NAMENODE_DIR/current/VERSION" ] && [ -f "$NAMENODE_DIR/current/fsimage" ]; then
  echo "Hadoop namenode appears to be formatted: skipping"
else
  echo "Formatting ephemeral HDFS namenode..."
  $EPHEMERAL_HDFS/bin/hadoop namenode -format
fi

echo "Starting ephemeral HDFS..."
# This is different depending on version. Simple hack: just try both.
$EPHEMERAL_HDFS/sbin/start-dfs.sh

