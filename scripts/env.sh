#!/usr/bin/env bash
# scripts/env.sh: Environment configuration for bd-search-engine
# Meant to be sourced from bash or zsh. Must not use exit.

export BD_HOME="${BD_HOME:-$HOME/.local/opt/bd-search-engine}"

if [ ! -x "${JAVA_HOME:-}/bin/java" ] && command -v java >/dev/null 2>&1; then
    export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
fi

_JAVA_BIN=""
if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
    _JAVA_BIN="${JAVA_HOME}/bin/java"
elif command -v java >/dev/null 2>&1; then
    _JAVA_BIN="java"
fi

if [ -n "$_JAVA_BIN" ]; then
    _JAVA_VER=$("$_JAVA_BIN" -version 2>&1 | head -n 1)
    if ! echo "$_JAVA_VER" | grep -Eq 'version "11[."]'; then
        echo "Warning: JAVA_HOME is not Java 11 (detected: ${_JAVA_VER})" >&2
    fi
else
    echo "Warning: Java not found on PATH or JAVA_HOME" >&2
fi
unset _JAVA_BIN _JAVA_VER

export HADOOP_HOME="$BD_HOME/hadoop-3.3.6"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="$BD_HOME/spark-3.3.4-bin-hadoop3"
export ICEBERG_JAR="$BD_HOME/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar"
export SPARK_CONF_FILE="$BD_HOME/conf/iceberg.conf"
export PYSPARK_PYTHON="$BD_HOME/venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$BD_HOME/venv/bin/python"

export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$BD_HOME/venv/bin:$PATH"
