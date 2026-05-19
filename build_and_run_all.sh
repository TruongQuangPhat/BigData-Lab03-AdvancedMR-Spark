#!/bin/bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="$PROJECT_ROOT/results"
LOG_DIR="$PROJECT_ROOT/logs"

INPUT_PATH="/lab03/input/Amazon_Sale_Report.csv"

mkdir -p "$RESULT_DIR" "$LOG_DIR"

echo "============================================================"
echo "LAB 03 - BUILD & RUN"
echo "============================================================"

# ============================================================
# Resolve SPARK_HOME
# ============================================================

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

# ============================================================
# Resolve HDFS URI
# ============================================================

HDFS_URI="$(hdfs getconf -confKey fs.defaultFS)"

if [[ -z "$HDFS_URI" ]]; then
  echo "THẤT BẠI: Không lấy được fs.defaultFS từ cấu hình Hadoop."
  exit 1
fi

SPARK_INPUT_PATH="${HDFS_URI}/lab03/input/Amazon_Sale_Report.csv"
SPARK_TASK21_OUTPUT_PATH="${HDFS_URI}/lab03/output/Task_2-1.parquet"
SPARK_TASK22_OUTPUT_PATH="${HDFS_URI}/lab03/output/Task_2-2.parquet"

export HADOOP_CLASSPATH="$(hadoop classpath):/usr/share/scala/lib/scala-library.jar"
export SPARK_CLASSPATH="$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')"

# ============================================================
# Prepare HDFS input
# ============================================================

if ! hadoop fs -mkdir -p /lab03/input/ >/dev/null 2>&1; then
  echo "THẤT BẠI: Không tạo được thư mục input trên HDFS."
  exit 1
fi

if ! hadoop fs -put -f "$PROJECT_ROOT/data/Amazon_Sale_Report.csv" /lab03/input/ >/dev/null 2>&1; then
  echo "THẤT BẠI: Không upload được dữ liệu lên HDFS."
  exit 1
fi

echo "THÀNH CÔNG: Chuẩn bị dữ liệu HDFS"

# ============================================================
# Task 1-1
# ============================================================

run_task_1_1() {
  local TASK_NAME="Task_1-1"
  local SRC_DIR="$PROJECT_ROOT/src/Task_1-1"
  local JAR_NAME="SlidingWindowJob.jar"
  local MAIN_CLASS="lab03.SlidingWindowJob"
  local OUTPUT_PATH="/lab03/output/task1-1"
  local LOCAL_OUTPUT="$RESULT_DIR/Task_1-1.csv"

  echo "Đang chạy $TASK_NAME..."

  if ! (
    cd "$SRC_DIR"

    mkdir -p classes
    rm -rf classes/*
    rm -f "$JAR_NAME"

    scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-1.scala
    jar -cvf "$JAR_NAME" -C classes .

    hadoop fs -rm -r -f "$OUTPUT_PATH"

    hadoop jar "$JAR_NAME" "$MAIN_CLASS" \
      "$INPUT_PATH" \
      "$OUTPUT_PATH"
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME"
    exit 1
  fi

  if ! (
    rm -f "$LOCAL_OUTPUT"
    hadoop fs -getmerge "$OUTPUT_PATH" "$LOCAL_OUTPUT"
    sed -i '2,${/^State,TargetDate/d}' "$LOCAL_OUTPUT"
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME - không lấy được kết quả về local."
    exit 1
  fi

  echo "THÀNH CÔNG: $TASK_NAME -> results/Task_1-1.csv"
}

# ============================================================
# Task 1-2
# ============================================================

run_task_1_2() {
  local TASK_NAME="Task_1-2"
  local SRC_DIR="$PROJECT_ROOT/src/Task_1-2"
  local JAR_NAME="MedianVarietyJob.jar"
  local MAIN_CLASS="lab03.MedianVarietyJob"
  local TEMP_PATH="/lab03/output/task1-2-temp"
  local OUTPUT_PATH="/lab03/output/task1-2"
  local LOCAL_OUTPUT="$RESULT_DIR/Task_1-2.csv"

  echo "Đang chạy $TASK_NAME..."

  if ! (
    cd "$SRC_DIR"

    mkdir -p classes
    rm -rf classes/*
    rm -f "$JAR_NAME"

    scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-2.scala
    jar -cvf "$JAR_NAME" -C classes .

    hadoop fs -rm -r -f "$TEMP_PATH"
    hadoop fs -rm -r -f "$OUTPUT_PATH"

    hadoop jar "$JAR_NAME" "$MAIN_CLASS" \
      "$INPUT_PATH" \
      "$TEMP_PATH" \
      "$OUTPUT_PATH"
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME"
    exit 1
  fi

  if ! (
    rm -f "$LOCAL_OUTPUT"
    hadoop fs -getmerge "$OUTPUT_PATH" "$LOCAL_OUTPUT"
    sed -i '2,${/^Month,State/d}' "$LOCAL_OUTPUT"
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME - không lấy được kết quả về local."
    exit 1
  fi

  echo "THÀNH CÔNG: $TASK_NAME -> results/Task_1-2.csv"
}

# ============================================================
# Task 2-1
# ============================================================

run_task_2_1() {
  local TASK_NAME="Task_2-1"
  local SRC_DIR="$PROJECT_ROOT/src/Task_2-1"
  local JAR_NAME="SparkTask21.jar"
  local MAIN_CLASS="lab3.task21.SparkTask21"
  local OUTPUT_PATH="/lab03/output/Task_2-1.parquet"
  local STAGING_PATH="/lab03/output/Task_2-1.parquet_staging"
  local LOCAL_OUTPUT="$RESULT_DIR/Task_2-1.parquet"

  echo "Đang chạy $TASK_NAME..."

  if ! (
    cd "$SRC_DIR"

    mkdir -p classes
    rm -rf classes/*
    rm -f "$JAR_NAME"

    scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-1.scala
    jar -cvf "$JAR_NAME" -C classes lab3

    hadoop fs -rm -r -f "$OUTPUT_PATH"
    hadoop fs -rm -r -f "$STAGING_PATH"

    spark-submit \
      --class "$MAIN_CLASS" \
      --master local[*] \
      --conf "spark.ui.showConsoleProgress=false" \
      "$JAR_NAME" \
      "$SPARK_INPUT_PATH" \
      "$SPARK_TASK21_OUTPUT_PATH"
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME"
    exit 1
  fi

  if ! (
    rm -rf "$LOCAL_OUTPUT"
    hadoop fs -get "$OUTPUT_PATH" "$LOCAL_OUTPUT"
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME - không lấy được kết quả về local."
    exit 1
  fi

  echo "THÀNH CÔNG: $TASK_NAME -> results/Task_2-1.parquet"
}

# ============================================================
# Task 2-2
# ============================================================

run_task_2_2() {
  local TASK_NAME="Task_2-2"
  local SRC_DIR="$PROJECT_ROOT/src/Task_2-2"
  local JAR_NAME="SparkTask22.jar"
  local MAIN_CLASS="Task22"
  local OUTPUT_PATH="/lab03/output/Task_2-2.parquet"
  local STAGING_PATH="/lab03/output/Task_2-2.parquet_staging"
  local LOCAL_OUTPUT="$RESULT_DIR/Task_2-2.parquet"
  local STATS_LOG="$LOG_DIR/task_2-2_stats.log"

  echo "Đang chạy $TASK_NAME..."

  if ! (
    cd "$SRC_DIR"

    mkdir -p classes
    rm -rf classes/*
    rm -f "$JAR_NAME"

    scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-2.scala
    jar -cvf "$JAR_NAME" -C classes .

    hadoop fs -rm -r -f "$OUTPUT_PATH"
    hadoop fs -rm -r -f "$STAGING_PATH"

    spark-submit \
      --class "$MAIN_CLASS" \
      --master local[*] \
      --conf "spark.ui.showConsoleProgress=false" \
      "$JAR_NAME" \
      "$SPARK_INPUT_PATH" \
      "$SPARK_TASK22_OUTPUT_PATH"
  ) > "$STATS_LOG" 2>&1; then
    echo "THẤT BẠI: $TASK_NAME. Xem chi tiết tại logs/task_2-2_stats.log"
    exit 1
  fi

  if ! (
    rm -rf "$LOCAL_OUTPUT"
    hadoop fs -get "$OUTPUT_PATH" "$LOCAL_OUTPUT"
  ) >/dev/null 2>&1; then
    echo "THẤT BẠI: $TASK_NAME - không lấy được kết quả về local."
    exit 1
  fi

  echo "THÀNH CÔNG: $TASK_NAME -> results/Task_2-2.parquet"
  echo "THÀNH CÔNG: Log Task_2-2 -> logs/task_2-2_stats.log"
}

# ============================================================
# Main
# ============================================================

run_task_1_1
run_task_1_2
run_task_2_1
run_task_2_2

echo "============================================================"
echo "HOÀN TẤT BUILD & RUN"
echo "Kết quả:"
echo "- results/Task_1-1.csv"
echo "- results/Task_1-2.csv"
echo "- results/Task_2-1.parquet"
echo "- results/Task_2-2.parquet"
echo "- logs/task_2-2_stats.log"
echo "============================================================"
