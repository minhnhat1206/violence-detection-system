#!/bin/bash
set -euo pipefail

HIVE_CONF_DIR=${HIVE_CONF_DIR:-/opt/hive/conf}
HIVE_HOME=${HIVE_HOME:-/opt/hive}
ACTION=${HIVE_SCHEMA_ACTION:-none}

WAIT_FOR_HOST=${WAIT_FOR_HOST:-mysql}
WAIT_FOR_PORT=${WAIT_FOR_PORT:-3306}
WAIT_TIMEOUT=${WAIT_TIMEOUT:-120}

MYSQL_USER=${METASTORE_DB_USER:-root}
MYSQL_PASS=${METASTORE_DB_PASSWORD:-root}
MYSQL_DB=${METASTORE_DB_NAME:-metastore}

echo "[entrypoint] Hive Metastore startup wrapper"
echo "[entrypoint] HIVE_CONF_DIR=${HIVE_CONF_DIR}, ACTION=${ACTION}"

wait_for_mysql() {
  echo "[entrypoint] waiting for ${WAIT_FOR_HOST}:${WAIT_FOR_PORT} (timeout ${WAIT_TIMEOUT}s)..."
  local n=0
  while ! nc -z ${WAIT_FOR_HOST} ${WAIT_FOR_PORT}; do
    n=$((n+1))
    if [ $n -ge ${WAIT_TIMEOUT} ]; then
      echo "[entrypoint] timeout waiting for mysql" >&2
      return 1
    fi
    sleep 1
  done
  echo "[entrypoint] mysql reachable"
}

wait_for_mysql

run_schematool() {
  local mode="$1"
  echo "[entrypoint] Running schematool -dbType mysql -${mode} (using hive-site.xml)"
  "${HIVE_HOME}/bin/schematool" -dbType mysql -"$mode"
}

if [ "$ACTION" = "init" ]; then
  echo "[entrypoint] Checking if Hive schema exists..."
  if ! mysql -h ${WAIT_FOR_HOST} -u${MYSQL_USER} -p${MYSQL_PASS} ${MYSQL_DB} -e "SELECT * FROM VERSION;" >/dev/null 2>&1; then
    echo "[entrypoint] Hive schema not found, initializing..."
    run_schematool initSchema
  else
    echo "[entrypoint] Hive schema already exists, skipping init"
  fi
elif [ "$ACTION" = "upgrade" ]; then
  run_schematool upgradeSchema
else
  echo "[entrypoint] HIVE_SCHEMA_ACTION=none, skipping schematool"
fi

echo "[entrypoint] Starting Hive Metastore service..."
exec "$@"
