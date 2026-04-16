#!/usr/bin/env bash

set -e

usage() {
  echo "Usage: $0 {start|stop|restart|cleanup|reset}"
  exit 1
}

cleanup() {
  echo "Cleaning HDFS data..."
  rm -rf "$HOME"/hadoopdata/hdfs/*
  hdfs namenode -format
}

start() {
  echo "Starting services..."
  "$HADOOP_HOME"/sbin/start-dfs.sh
  "$HADOOP_HOME"/sbin/start-yarn.sh
  # $SPARK_HOME/sbin/start-all.sh
  # hdfs dfsadmin -safemode leave
  # sudo systemctl start elasticsearch.service
}

stop() {
  echo "Stopping services..."
  # $SPARK_HOME/sbin/stop-all.sh
  "$HADOOP_HOME"/sbin/stop-yarn.sh
  "$HADOOP_HOME"/sbin/stop-dfs.sh
  # sudo systemctl stop elasticsearch.service
}

case "${1:-}" in
  start)   start ;;
  stop)    stop ;;
  restart) stop; cleanup; start ;;
  cleanup) cleanup ;;
  reset)   stop; cleanup; start ;;  # alias for restart
  *)       usage ;;
esac
