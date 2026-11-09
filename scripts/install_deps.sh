#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BD_HOME="${BD_HOME:-$HOME/.local/opt/bd-search-engine}"
mkdir -p "$BD_HOME"
mkdir -p "$BD_HOME/downloads"

# 1. Verify Java OpenJDK 11
if [ -n "${JAVA_HOME:-}" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
elif command -v java >/dev/null 2>&1; then
    JAVA_CMD="java"
else
    echo "Error: Java is missing or not found on PATH / JAVA_HOME." >&2
    exit 1
fi

JAVA_VERSION_OUTPUT="$("$JAVA_CMD" -version 2>&1 || true)"
if ! echo "$JAVA_VERSION_OUTPUT" | grep -Eq 'version "11[."]'; then
    echo "Error: Java major version 11 is required, but detected:" >&2
    echo "$JAVA_VERSION_OUTPUT" >&2
    exit 1
fi
echo "Java 11 verified: $(echo "$JAVA_VERSION_OUTPUT" | head -n 1)"

# 2. Verify Python 3.10
if ! command -v python3.10 >/dev/null 2>&1; then
    echo "Error: python3.10 is missing or not found on PATH." >&2
    exit 1
fi
echo "Python 3.10 verified: $(python3.10 --version)"

# 3. Checksum verification function
verify_checksum() {
    local target_file="$1"
    local checksum_file="$2"
    local algo="$3"

    python3.10 - "$target_file" "$checksum_file" "$algo" <<'PYEOF'
import sys
import re
import hashlib

target_file = sys.argv[1]
checksum_file = sys.argv[2]
algo = sys.argv[3].lower()

try:
    with open(checksum_file, 'r', encoding='utf-8', errors='replace') as f:
        raw = f.read().strip()
except Exception as e:
    sys.stderr.write(f"Error reading checksum file {checksum_file}: {e}\n")
    sys.exit(1)

# Normalize formats:
# Format A: SHA512 (file) = hex
if '=' in raw:
    raw = raw.split('=', 1)[1]
# Format B: file: HEX HEX ...
elif ':' in raw:
    raw = raw.split(':', 1)[1]
else:
    # Format C: <hex>  <filename> or raw hex
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
    if lines:
        parts = lines[0].split()
        if parts:
            raw = parts[0]

expected_hex = re.sub(r'[^0-9a-fA-F]', '', raw).lower()
expected_len = 128 if algo == 'sha512' else 40

if len(expected_hex) != expected_len:
    sys.stderr.write(
        f"Error: parsed {algo} checksum length is {len(expected_hex)}, "
        f"expected {expected_len} in {checksum_file}\n"
    )
    sys.exit(1)

if algo == 'sha512':
    hasher = hashlib.sha512()
elif algo == 'sha1':
    hasher = hashlib.sha1()
else:
    sys.stderr.write(f"Error: unsupported algorithm {algo}\n")
    sys.exit(1)

try:
    with open(target_file, 'rb') as f:
        while True:
            chunk = f.read(1048576)
            if not chunk:
                break
            hasher.update(chunk)
except Exception as e:
    sys.stderr.write(f"Error reading target file {target_file}: {e}\n")
    sys.exit(1)

actual_hex = hasher.hexdigest().lower()

if expected_hex != actual_hex:
    sys.stderr.write(
        f"Checksum mismatch for {target_file}!\n"
        f"Expected ({algo}): {expected_hex}\n"
        f"Actual   ({algo}): {actual_hex}\n"
    )
    sys.exit(1)

print(f"Verified {algo} checksum for {target_file}")
PYEOF
}

# 4. Download and verification function
download_and_verify() {
    local url="$1"
    local checksum_url="$2"
    local filename="$3"
    local checksum_filename="$4"
    local algo="$5"

    local target_file="$BD_HOME/downloads/$filename"
    local checksum_file="$BD_HOME/downloads/$checksum_filename"
    local tmp_file="$BD_HOME/downloads/$filename.tmp"
    local tmp_checksum_file="$BD_HOME/downloads/$checksum_filename.tmp"

    echo "Downloading checksum file for $filename..."
    curl -f -s -S -L "$checksum_url" -o "$tmp_checksum_file"
    mv "$tmp_checksum_file" "$checksum_file"

    if [ -f "$target_file" ]; then
        echo "Found existing $filename, checking checksum..."
        if verify_checksum "$target_file" "$checksum_file" "$algo" >/dev/null 2>&1; then
            echo "$filename already downloaded and verified. Skipping download."
            return 0
        else
            echo "Existing $filename failed checksum verification. Removing and re-downloading..."
            rm -f "$target_file"
        fi
    fi

    if [ -f "$tmp_file" ]; then
        echo "Resuming download of $filename..."
        if ! curl -C - -f -s -S -L "$url" -o "$tmp_file"; then
            echo "Resume failed. Restarting download of $filename from scratch..."
            rm -f "$tmp_file"
            curl -f -s -S -L "$url" -o "$tmp_file"
        fi
    else
        echo "Downloading $filename..."
        curl -f -s -S -L "$url" -o "$tmp_file"
    fi

    echo "Verifying checksum for $filename..."
    if ! verify_checksum "$tmp_file" "$checksum_file" "$algo"; then
        echo "Error: Checksum verification failed for $filename. Removing corrupted temporary file." >&2
        rm -f "$tmp_file"
        exit 1
    fi

    mv "$tmp_file" "$target_file"
    echo "Successfully downloaded and verified $filename."
}

# 5. Download components
echo "=== Checking and Downloading Dependencies ==="
download_and_verify \
    "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz" \
    "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz.sha512" \
    "hadoop-3.3.6.tar.gz" \
    "hadoop-3.3.6.tar.gz.sha512" \
    "sha512"

download_and_verify \
    "https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz" \
    "https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz.sha512" \
    "spark-3.3.4-bin-hadoop3.tgz" \
    "spark-3.3.4-bin-hadoop3.tgz.sha512" \
    "sha512"

download_and_verify \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.8.1/iceberg-spark-runtime-3.3_2.12-1.8.1.jar" \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.8.1/iceberg-spark-runtime-3.3_2.12-1.8.1.jar.sha1" \
    "iceberg-spark-runtime-3.3_2.12-1.8.1.jar" \
    "iceberg-spark-runtime-3.3_2.12-1.8.1.jar.sha1" \
    "sha1"

# 6. Extract Hadoop 3.3.6
echo "=== Extracting Hadoop ==="
HADOOP_DIR="$BD_HOME/hadoop-3.3.6"
if [ -x "$HADOOP_DIR/bin/hadoop" ]; then
    echo "Hadoop 3.3.6 already extracted and verified. Skipping."
else
    echo "Extracting Hadoop 3.3.6 to $HADOOP_DIR..."
    rm -rf "$HADOOP_DIR.tmp" "$HADOOP_DIR"
    mkdir -p "$HADOOP_DIR.tmp"
    tar -xzf "$BD_HOME/downloads/hadoop-3.3.6.tar.gz" -C "$HADOOP_DIR.tmp" --strip-components=1
    mv "$HADOOP_DIR.tmp" "$HADOOP_DIR"
    if [ ! -x "$HADOOP_DIR/bin/hadoop" ]; then
        echo "Error: Verification failed; $HADOOP_DIR/bin/hadoop not found." >&2
        exit 1
    fi
    echo "Hadoop 3.3.6 extracted and verified."
fi

# 7. Extract Spark 3.3.4
echo "=== Extracting Spark ==="
SPARK_DIR="$BD_HOME/spark-3.3.4-bin-hadoop3"
if [ -x "$SPARK_DIR/bin/spark-submit" ]; then
    echo "Spark 3.3.4 already extracted and verified. Skipping."
else
    echo "Extracting Spark 3.3.4 to $SPARK_DIR..."
    rm -rf "$SPARK_DIR.tmp" "$SPARK_DIR"
    mkdir -p "$SPARK_DIR.tmp"
    tar -xzf "$BD_HOME/downloads/spark-3.3.4-bin-hadoop3.tgz" -C "$SPARK_DIR.tmp" --strip-components=1
    mv "$SPARK_DIR.tmp" "$SPARK_DIR"
    if [ ! -x "$SPARK_DIR/bin/spark-submit" ]; then
        echo "Error: Verification failed; $SPARK_DIR/bin/spark-submit not found." >&2
        exit 1
    fi
    echo "Spark 3.3.4 extracted and verified."
fi

# 8. Place Iceberg jar
echo "=== Installing Iceberg Runtime JAR ==="
mkdir -p "$BD_HOME/iceberg"
ICEBERG_JAR_PATH="$BD_HOME/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar"
if [ -f "$ICEBERG_JAR_PATH" ] && verify_checksum "$ICEBERG_JAR_PATH" "$BD_HOME/downloads/iceberg-spark-runtime-3.3_2.12-1.8.1.jar.sha1" "sha1" >/dev/null 2>&1; then
    echo "Iceberg jar already in place and verified. Skipping."
else
    echo "Copying Iceberg jar to $ICEBERG_JAR_PATH..."
    cp "$BD_HOME/downloads/iceberg-spark-runtime-3.3_2.12-1.8.1.jar" "$ICEBERG_JAR_PATH.tmp"
    verify_checksum "$ICEBERG_JAR_PATH.tmp" "$BD_HOME/downloads/iceberg-spark-runtime-3.3_2.12-1.8.1.jar.sha1" "sha1"
    mv "$ICEBERG_JAR_PATH.tmp" "$ICEBERG_JAR_PATH"
    echo "Iceberg jar placed and verified."
fi

# 9. Setup Python Virtual Environment and install dependencies
echo "=== Setting up Python Virtual Environment ==="
VENV_DIR="$BD_HOME/venv"
if [ ! -x "$VENV_DIR/bin/python" ]; then
    echo "Creating virtual environment at $VENV_DIR using python3.10..."
    python3.10 -m venv "$VENV_DIR"
fi

if [ -f "$REPO_ROOT/requirements.txt" ] && [ -s "$REPO_ROOT/requirements.txt" ]; then
    echo "Installing Python dependencies from requirements.txt..."
    "$VENV_DIR/bin/pip" install -r "$REPO_ROOT/requirements.txt"
elif [ -f "$REPO_ROOT/requirements.in" ]; then
    echo "requirements.txt not found or empty; bootstrapping from requirements.in..."
    "$VENV_DIR/bin/pip" install -r "$REPO_ROOT/requirements.in"
else
    echo "Error: Neither requirements.txt nor requirements.in found in $REPO_ROOT." >&2
    exit 1
fi

# 10. Render Iceberg configuration
echo "=== Rendering Iceberg Configuration ==="
mkdir -p "$BD_HOME/conf"
ICEBERG_WAREHOUSE_VAL="${ICEBERG_WAREHOUSE:-hdfs://localhost:9000/warehouse}"

sed \
    -e "s|<warehouse>|$ICEBERG_WAREHOUSE_VAL|g" \
    -e "s|<absolute path of the Iceberg jar>|$ICEBERG_JAR_PATH|g" \
    "$REPO_ROOT/conf/iceberg.conf.template" > "$BD_HOME/conf/iceberg.conf"
echo "Iceberg configuration rendered to $BD_HOME/conf/iceberg.conf."

echo "=== Dependency installation completed successfully ==="
