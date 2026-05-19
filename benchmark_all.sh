#!/bin/bash
set -euo pipefail

RUNS=5
WARMUP_RUNS=1

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"

INPUT_PATH="/lab03/input/Amazon_Sale_Report.csv"

mkdir -p "$LOG_DIR"

echo "============================================================"
echo "LAB 03 - BENCHMARK"
echo "============================================================"

if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  :

elif [[ -x "/opt/spark/bin/spark-submit" ]]; then
  export SPARK_HOME="/opt/spark"

elif [[ -x "$HOME/spark/bin/spark-submit" ]]; then
  export SPARK_HOME="$HOME/spark"

elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT_PATH="$(command -v spark-submit)"
  export SPARK_HOME="$(cd "$(dirname "$SPARK_SUBMIT_PATH")/.." && pwd)"

else
  echo "THẤT BẠI: Không tìm thấy Spark. Hãy cài Spark hoặc cấu hình SPARK_HOME."
  exit 1
fi

export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

if [[ ! -d "$SPARK_HOME/jars" ]]; then
  echo "THẤT BẠI: SPARK_HOME không hợp lệ: $SPARK_HOME"
  exit 1
fi

HDFS_URI="$(hdfs getconf -confKey fs.defaultFS)"

if [[ -z "$HDFS_URI" ]]; then
  echo "THẤT BẠI: Không lấy được fs.defaultFS từ cấu hình Hadoop."
  exit 1
fi

SPARK_INPUT_PATH="${HDFS_URI}/lab03/input/Amazon_Sale_Report.csv"
SPARK_TASK21_OUTPUT_PATH="${HDFS_URI}/lab03/output/Task_2-1.parquet"
SPARK_TASK22_OUTPUT_PATH="${HDFS_URI}/lab03/output/Task_2-2.parquet"

SCALA_LIBRARY_JAR="${SCALA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which scalac)")")")}/lib/scala-library.jar"

if [[ ! -f "$SCALA_LIBRARY_JAR" ]]; then
  echo "THẤT BẠI: Không tìm thấy scala-library.jar tương ứng với scalac hiện tại."
  echo "SCALA_LIBRARY_JAR=$SCALA_LIBRARY_JAR"
  exit 1
fi

export HADOOP_CLASSPATH="$(hadoop classpath):$SCALA_LIBRARY_JAR"
export SPARK_CLASSPATH="$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')"

if ! hadoop fs -mkdir -p /lab03/input/ >/dev/null 2>&1; then
  echo "THẤT BẠI: Không tạo được thư mục input trên HDFS."
  exit 1
fi

if ! hadoop fs -put -f "$PROJECT_ROOT/data/Amazon_Sale_Report.csv" /lab03/input/ >/dev/null 2>&1; then
  echo "THẤT BẠI: Không upload được dữ liệu lên HDFS."
  exit 1
fi

echo "THÀNH CÔNG: Chuẩn bị dữ liệu HDFS"

measure_command() {
  local label="$1"
  local output_file="$2"
  shift 2

  local start_ns
  local end_ns
  local runtime

  start_ns="$(date +%s%N)"

  if "$@" >/dev/null 2>&1; then
    end_ns="$(date +%s%N)"
    runtime="$(python3 - <<PY
start_ns = int("$start_ns")
end_ns = int("$end_ns")
print(round((end_ns - start_ns) / 1_000_000_000, 6))
PY
)"
    echo "$runtime" >> "$output_file"
    return 0
  else
    echo "THẤT BẠI: $label"
    return 1
  fi
}

write_json_log() {
  local task_name="$1"
  local main_class="$2"
  local framework="$3"
  local input_path="$4"
  local output_path="$5"
  local warmup_file="$6"
  local tmp_file="$7"
  local log_file="$8"

  python3 - <<PY
import json
import statistics
from datetime import datetime

task_name = "$task_name"
main_class = "$main_class"
framework = "$framework"
input_path = "$input_path"
output_path = "$output_path"
warmup_file = "$warmup_file"
tmp_file = "$tmp_file"
log_file = "$log_file"

warmup = []
with open(warmup_file, "r", encoding="utf-8") as f:
    for idx, line in enumerate(f, start=1):
        runtime = float(line.strip())
        warmup.append({
            "run": idx,
            "runtime_seconds": runtime
        })

runs = []
with open(tmp_file, "r", encoding="utf-8") as f:
    for idx, line in enumerate(f, start=1):
        runtime = float(line.strip())
        runs.append({
            "run": idx,
            "runtime_seconds": runtime
        })

values = [item["runtime_seconds"] for item in runs]
warmup_values = [item["runtime_seconds"] for item in warmup]

result = {
    "task": task_name,
    "framework": framework,
    "benchmark_type": "job_runtime",
    "description": "Benchmark measures submitted job execution time. Warm-up runs are recorded separately and are not included in official statistics. Compilation, JAR packaging, HDFS input preparation, result copying, and local post-processing are excluded.",
    "input_path": input_path,
    "output_path": output_path,
    "main_class": main_class,
    "warmup": {
        "number_of_runs": len(warmup),
        "runs": warmup,
        "statistics": {
            "mean_seconds": round(statistics.mean(warmup_values), 6) if warmup_values else 0.0,
            "min_seconds": round(min(warmup_values), 6) if warmup_values else 0.0,
            "max_seconds": round(max(warmup_values), 6) if warmup_values else 0.0
        },
        "included_in_official_statistics": False
    },
    "measured": {
        "number_of_runs": len(runs),
        "runs": runs,
        "statistics": {
            "mean_seconds": round(statistics.mean(values), 6),
            "stddev_seconds": round(statistics.stdev(values), 6) if len(values) >= 2 else 0.0,
            "min_seconds": round(min(values), 6),
            "max_seconds": round(max(values), 6)
        }
    },
    "created_at": datetime.now().isoformat(timespec="seconds")
}

with open(log_file, "w", encoding="utf-8") as f:
    json.dump(result, f, indent=2, ensure_ascii=False)
PY
}

benchmark_task_1_1() {
  local TASK_NAME="Task_1-1"
  local SRC_DIR="$PROJECT_ROOT/src/Task_1-1"
  local JAR_NAME="SlidingWindowJob.jar"
  local MAIN_CLASS="lab03.SlidingWindowJob"
  local OUTPUT_PATH="/lab03/output/task1-1"
  local WARMUP_FILE="$LOG_DIR/task1_1_warmup.tmp"
  local TMP_FILE="$LOG_DIR/task1_1_times.tmp"
  local LOG_FILE="$LOG_DIR/Task_1-1.json"

  echo "Đang benchmark $TASK_NAME..."

  rm -f "$WARMUP_FILE" "$TMP_FILE"

  if ! (
    cd "$SRC_DIR"

    mkdir -p classes
    rm -rf classes/*
    rm -f "$JAR_NAME"

    scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-1.scala

    (
      cd classes
      jar xf "$SCALA_LIBRARY_JAR"
    )

    jar -cvf "$JAR_NAME" -C classes .
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME - lỗi biên dịch hoặc đóng gói."
    exit 1
  fi

  for i in $(seq 1 "$WARMUP_RUNS"); do
    hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true

    measure_command \
      "$TASK_NAME warm-up $i/$WARMUP_RUNS" \
      "$WARMUP_FILE" \
      hadoop jar "$SRC_DIR/$JAR_NAME" "$MAIN_CLASS" \
        "$INPUT_PATH" \
        "$OUTPUT_PATH"
  done

  hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true

  for i in $(seq 1 "$RUNS"); do
    hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true

    measure_command \
      "$TASK_NAME lần chạy chính thức $i/$RUNS" \
      "$TMP_FILE" \
      hadoop jar "$SRC_DIR/$JAR_NAME" "$MAIN_CLASS" \
        "$INPUT_PATH" \
        "$OUTPUT_PATH"
  done

  write_json_log \
    "$TASK_NAME" \
    "$MAIN_CLASS" \
    "Hadoop MapReduce" \
    "$INPUT_PATH" \
    "$OUTPUT_PATH" \
    "$WARMUP_FILE" \
    "$TMP_FILE" \
    "$LOG_FILE"

  rm -f "$WARMUP_FILE" "$TMP_FILE"

  echo "THÀNH CÔNG: $TASK_NAME -> logs/Task_1-1.json"
}

benchmark_task_1_2() {
  local TASK_NAME="Task_1-2"
  local SRC_DIR="$PROJECT_ROOT/src/Task_1-2"
  local JAR_NAME="MedianVarietyJob.jar"
  local MAIN_CLASS="lab03.MedianVarietyJob"
  local TEMP_PATH="/lab03/output/task1-2-temp"
  local OUTPUT_PATH="/lab03/output/task1-2"
  local WARMUP_FILE="$LOG_DIR/task1_2_warmup.tmp"
  local TMP_FILE="$LOG_DIR/task1_2_times.tmp"
  local LOG_FILE="$LOG_DIR/Task_1-2.json"

  echo "Đang benchmark $TASK_NAME..."

  rm -f "$WARMUP_FILE" "$TMP_FILE"

  if ! (
    cd "$SRC_DIR"

    mkdir -p classes
    rm -rf classes/*
    rm -f "$JAR_NAME"

    scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-2.scala

    (
      cd classes
      jar xf "$SCALA_LIBRARY_JAR"
    )

    jar -cvf "$JAR_NAME" -C classes .
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME - lỗi biên dịch hoặc đóng gói."
    exit 1
  fi

  for i in $(seq 1 "$WARMUP_RUNS"); do
    hadoop fs -rm -r -f "$TEMP_PATH" >/dev/null 2>&1 || true
    hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true

    measure_command \
      "$TASK_NAME warm-up $i/$WARMUP_RUNS" \
      "$WARMUP_FILE" \
      hadoop jar "$SRC_DIR/$JAR_NAME" "$MAIN_CLASS" \
        "$INPUT_PATH" \
        "$TEMP_PATH" \
        "$OUTPUT_PATH"
  done

  hadoop fs -rm -r -f "$TEMP_PATH" >/dev/null 2>&1 || true
  hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true

  for i in $(seq 1 "$RUNS"); do
    hadoop fs -rm -r -f "$TEMP_PATH" >/dev/null 2>&1 || true
    hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true

    measure_command \
      "$TASK_NAME lần chạy chính thức $i/$RUNS" \
      "$TMP_FILE" \
      hadoop jar "$SRC_DIR/$JAR_NAME" "$MAIN_CLASS" \
        "$INPUT_PATH" \
        "$TEMP_PATH" \
        "$OUTPUT_PATH"
  done

  write_json_log \
    "$TASK_NAME" \
    "$MAIN_CLASS" \
    "Hadoop MapReduce" \
    "$INPUT_PATH" \
    "$OUTPUT_PATH" \
    "$WARMUP_FILE" \
    "$TMP_FILE" \
    "$LOG_FILE"

  rm -f "$WARMUP_FILE" "$TMP_FILE"

  echo "THÀNH CÔNG: $TASK_NAME -> logs/Task_1-2.json"
}

benchmark_task_2_1() {
  local TASK_NAME="Task_2-1"
  local SRC_DIR="$PROJECT_ROOT/src/Task_2-1"
  local JAR_NAME="SparkTask21.jar"
  local MAIN_CLASS="lab3.task21.SparkTask21"
  local OUTPUT_PATH="/lab03/output/Task_2-1.parquet"
  local WARMUP_FILE="$LOG_DIR/task2_1_warmup.tmp"
  local TMP_FILE="$LOG_DIR/task2_1_times.tmp"
  local LOG_FILE="$LOG_DIR/Task_2-1.json"

  echo "Đang benchmark $TASK_NAME..."

  rm -f "$WARMUP_FILE" "$TMP_FILE"

  if ! (
    cd "$SRC_DIR"

    mkdir -p classes
    rm -rf classes/*
    rm -f "$JAR_NAME"

    scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-1.scala
    jar -cvf "$JAR_NAME" -C classes lab3
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME - lỗi biên dịch hoặc đóng gói."
    exit 1
  fi

  for i in $(seq 1 "$WARMUP_RUNS"); do
    hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true
    hadoop fs -rm -r -f "${OUTPUT_PATH}_staging" >/dev/null 2>&1 || true

    measure_command \
      "$TASK_NAME warm-up $i/$WARMUP_RUNS" \
      "$WARMUP_FILE" \
      spark-submit \
        --class "$MAIN_CLASS" \
        --master local[*] \
        --conf "spark.ui.showConsoleProgress=false" \
        "$SRC_DIR/$JAR_NAME" \
        "$SPARK_INPUT_PATH" \
        "$SPARK_TASK21_OUTPUT_PATH"
  done

  hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true
  hadoop fs -rm -r -f "${OUTPUT_PATH}_staging" >/dev/null 2>&1 || true

  for i in $(seq 1 "$RUNS"); do
    hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true
    hadoop fs -rm -r -f "${OUTPUT_PATH}_staging" >/dev/null 2>&1 || true

    measure_command \
      "$TASK_NAME lần chạy chính thức $i/$RUNS" \
      "$TMP_FILE" \
      spark-submit \
        --class "$MAIN_CLASS" \
        --master local[*] \
        --conf "spark.ui.showConsoleProgress=false" \
        "$SRC_DIR/$JAR_NAME" \
        "$SPARK_INPUT_PATH" \
        "$SPARK_TASK21_OUTPUT_PATH"
  done

  write_json_log \
    "$TASK_NAME" \
    "$MAIN_CLASS" \
    "Apache Spark" \
    "$SPARK_INPUT_PATH" \
    "$SPARK_TASK21_OUTPUT_PATH" \
    "$WARMUP_FILE" \
    "$TMP_FILE" \
    "$LOG_FILE"

  rm -f "$WARMUP_FILE" "$TMP_FILE"

  echo "THÀNH CÔNG: $TASK_NAME -> logs/Task_2-1.json"
}

benchmark_task_2_2() {
  local TASK_NAME="Task_2-2"
  local SRC_DIR="$PROJECT_ROOT/src/Task_2-2"
  local JAR_NAME="SparkTask22.jar"
  local MAIN_CLASS="Task22"
  local OUTPUT_PATH="/lab03/output/Task_2-2.parquet"
  local WARMUP_FILE="$LOG_DIR/task2_2_warmup.tmp"
  local TMP_FILE="$LOG_DIR/task2_2_times.tmp"
  local LOG_FILE="$LOG_DIR/Task_2-2.json"

  echo "Đang benchmark $TASK_NAME..."

  rm -f "$WARMUP_FILE" "$TMP_FILE"

  if ! (
    cd "$SRC_DIR"

    mkdir -p classes
    rm -rf classes/*
    rm -f "$JAR_NAME"

    scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-2.scala
    jar -cvf "$JAR_NAME" -C classes .
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME - lỗi biên dịch hoặc đóng gói."
    exit 1
  fi

  for i in $(seq 1 "$WARMUP_RUNS"); do
    hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true
    hadoop fs -rm -r -f "${OUTPUT_PATH}_staging" >/dev/null 2>&1 || true

    measure_command \
      "$TASK_NAME warm-up $i/$WARMUP_RUNS" \
      "$WARMUP_FILE" \
      spark-submit \
        --class "$MAIN_CLASS" \
        --master local[*] \
        --conf "spark.ui.showConsoleProgress=false" \
        "$SRC_DIR/$JAR_NAME" \
        "$SPARK_INPUT_PATH" \
        "$SPARK_TASK22_OUTPUT_PATH"
  done

  hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true
  hadoop fs -rm -r -f "${OUTPUT_PATH}_staging" >/dev/null 2>&1 || true

  for i in $(seq 1 "$RUNS"); do
    hadoop fs -rm -r -f "$OUTPUT_PATH" >/dev/null 2>&1 || true
    hadoop fs -rm -r -f "${OUTPUT_PATH}_staging" >/dev/null 2>&1 || true

    measure_command \
      "$TASK_NAME lần chạy chính thức $i/$RUNS" \
      "$TMP_FILE" \
      spark-submit \
        --class "$MAIN_CLASS" \
        --master local[*] \
        --conf "spark.ui.showConsoleProgress=false" \
        "$SRC_DIR/$JAR_NAME" \
        "$SPARK_INPUT_PATH" \
        "$SPARK_TASK22_OUTPUT_PATH"
  done

  write_json_log \
    "$TASK_NAME" \
    "$MAIN_CLASS" \
    "Apache Spark" \
    "$SPARK_INPUT_PATH" \
    "$SPARK_TASK22_OUTPUT_PATH" \
    "$WARMUP_FILE" \
    "$TMP_FILE" \
    "$LOG_FILE"

  rm -f "$WARMUP_FILE" "$TMP_FILE"

  echo "THÀNH CÔNG: $TASK_NAME -> logs/Task_2-2.json"
}

benchmark_task_1_1
benchmark_task_1_2
benchmark_task_2_1
benchmark_task_2_2

echo "============================================================"
echo "HOÀN TẤT BENCHMARK"
echo "Log benchmark:"
echo "- logs/Task_1-1.json"
echo "- logs/Task_1-2.json"
echo "- logs/Task_2-1.json"
echo "- logs/Task_2-2.json"
echo "============================================================"
