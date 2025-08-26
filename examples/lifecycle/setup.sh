#!/bin/bash

set -eu

SCRIPT_DIR=$(realpath "$(dirname "$0")")
VERSION=4.1.9
CASSANDRA_DIR="apache-cassandra-${VERSION}"
TARBALL_NAME="${CASSANDRA_DIR}-bin.tar.gz"
TARBALL_URL="https://dlcdn.apache.org/cassandra/${VERSION}/${TARBALL_NAME}"
NODE_DIR="${SCRIPT_DIR}/nodes/localhost"

SIDECAR_YAML="${SCRIPT_DIR}/conf/sidecar.yaml"
SIDECAR_YAML_TEMPLATE="${SCRIPT_DIR}/conf/sidecar.yaml.template"
CASSANDRA_HOME="${NODE_DIR}/opt/${CASSANDRA_DIR}"
CASSANDRA_LOG_DIR="${NODE_DIR}/var/log/cassandra"
CASSANDRA_CONF="${NODE_DIR}/etc/cassandra"
CASSANDRA_STORAGE_DIR="${NODE_DIR}/var/lib/cassandra"
SIDECAR_LIFECYCLE_DIR="${NODE_DIR}/var/lib/cassandra-sidecar/lifecycle"
TMP_DIR="${NODE_DIR}/tmp"

echo "Creating directories"
mkdir -p ${CASSANDRA_HOME} ${CASSANDRA_LOG_DIR} ${CASSANDRA_CONF} ${CASSANDRA_STORAGE_DIR} ${SIDECAR_LIFECYCLE_DIR} ${TMP_DIR}

if [ -f ${CASSANDRA_HOME}/bin/cassandra ]; then
  echo "Cassandra already installed at ${CASSANDRA_HOME}, skipping install"
else
  echo "Installing Cassandra at ${CASSANDRA_HOME}"
  echo "Downloading ${TARBALL_URL}"
  wget -P ${TMP_DIR} ${TARBALL_URL}

  echo "Extracting tarball"
  tar -xvzf ${TMP_DIR}/${TARBALL_NAME} -C $(dirname $CASSANDRA_HOME)
fi

echo "Creating configuration"
cp -r ${CASSANDRA_HOME}/conf/* ${CASSANDRA_CONF}
sed "s#\$cassandraHome#${CASSANDRA_HOME}#g" ${SIDECAR_YAML_TEMPLATE} > ${SIDECAR_YAML}
sed -i "s#\$baseDir#${NODE_DIR}#g" ${SIDECAR_YAML}
