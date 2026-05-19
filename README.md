# LAB 03 - ADVANCED MAPREDUCE AND SPARK STRUCTURED API

## 1. Mục đích

README này hướng dẫn cách chuẩn bị môi trường, chạy toàn bộ các task của Lab 03 và thực hiện benchmark thời gian chạy.

Lab 03 gồm bốn task:

- `Task_1-1`: Sliding Window bằng Hadoop MapReduce
- `Task_1-2`: Median Variety bằng Hadoop MapReduce
- `Task_2-1`: Cancelled Standard Order Qualification Percentage bằng Spark Structured API
- `Task_2-2`: Population Standard Deviation with Dynamic Percentiles bằng Spark Structured API

## 2. Yêu cầu môi trường

Môi trường đã được kiểm thử với cấu hình sau:

```text
Java: OpenJDK 8
Hadoop: Hadoop 3.x
Spark: Spark 3.5.x, build với Scala 2.12
Scala compiler: Scala 2.12.18
Python: Python 3
Hệ điều hành: Linux hoặc WSL
```

Điểm quan trọng là version Scala dùng để biên dịch phải khớp với version Scala của Spark runtime. Với Spark đang dùng Scala 2.12.18, nên dùng Scala compiler 2.12.18.

Kiểm tra môi trường:

```bash
java -version
scala -version
scalac -version
spark-submit --version 2>&1 | grep -i scala
hdfs getconf -confKey fs.defaultFS
```

Kết quả mong đợi:

```text
Scala code runner version 2.12.18
Scala compiler version 2.12.18
Using Scala version 2.12.18
```

## 3. Cấu trúc thư mục

```text
BigData-Lab03-AdvancedMR-Spark/
├── data/
│   └── Amazon_Sale_Report.csv
├── results/
│   ├── Task_1-1.csv
│   ├── Task_1-2.csv
│   ├── Task_2-1.parquet
│   └── Task_2-2.parquet
├── logs/
│   ├── Task_1-1.json
│   ├── Task_1-2.json
│   ├── Task_2-1.json
│   ├── Task_2-2.json
│   └── task_2-2_stats.log
├── src/
│   ├── Task_1-1/
│   │   └── Task_1-1.scala
│   ├── Task_1-2/
│   │   └── Task_1-2.scala
│   ├── Task_2-1/
│   │   └── Task_2-1.scala
│   └── Task_2-2/
│       └── Task_2-2.scala
├── build_and_run_all.sh
├── benchmark_all.sh
└── README.md
```

## 4. Chuẩn bị Hadoop và HDFS

Tất cả các lệnh dưới đây được chạy tại thư mục gốc của project.

```bash
cd ~/AI_Project/BigData-Lab03-AdvancedMR-Spark
```

Khởi động Hadoop:

```bash
start-dfs.sh
start-yarn.sh
```

Kiểm tra tiến trình:

```bash
jps
```

Cần có các tiến trình chính sau:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

Tạo input trên HDFS:

```bash
hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f data/Amazon_Sale_Report.csv /lab03/input/
hadoop fs -ls /lab03/input/
```

## 5. Lưu ý về Scala runtime cho MapReduce

Hai task MapReduce (`Task_1-1` và `Task_1-2`) được viết bằng Scala nhưng chạy trong Hadoop YARN container. Vì vậy, khi đóng gói JAR cho MapReduce, cần đưa `scala-library.jar` tương ứng với `scalac` hiện tại vào JAR.

Script `build_and_run_all.sh` và `benchmark_all.sh` đã xử lý việc này tự động bằng cách:

```bash
SCALA_LIBRARY_JAR="${SCALA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which scalac)")")")}/lib/scala-library.jar"
```

Sau đó, với các task MapReduce, script sẽ unpack Scala runtime vào thư mục `classes` trước khi tạo JAR:

```bash
cd classes
jar xf "$SCALA_LIBRARY_JAR"
cd ..
```

Không dùng hard-code:

```text
/usr/share/scala/lib/scala-library.jar
```

vì đường dẫn này có thể trỏ tới Scala 2.11 cũ và gây lỗi runtime khi code được compile bằng Scala 2.12.

## 6. Chạy toàn bộ project bằng script

Cấp quyền thực thi:

```bash
chmod +x build_and_run_all.sh
```

Chạy toàn bộ 4 task:

```bash
./build_and_run_all.sh
```

Script sẽ tự động thực hiện:

1. Kiểm tra và xác định `SPARK_HOME`
2. Lấy HDFS URI từ `fs.defaultFS`
3. Xác định `scala-library.jar` tương ứng với `scalac`
4. Upload dữ liệu vào `/lab03/input/`
5. Build và chạy `Task_1-1`
6. Build và chạy `Task_1-2`
7. Build và chạy `Task_2-1`
8. Build và chạy `Task_2-2`
9. Lấy kết quả cuối cùng về thư mục `results/`
10. Lưu log thống kê của `Task_2-2` vào `logs/task_2-2_stats.log`

Kết quả sau khi chạy thành công:

```text
results/Task_1-1.csv
results/Task_1-2.csv
results/Task_2-1.parquet
results/Task_2-2.parquet
logs/task_2-2_stats.log
```

Kiểm tra kết quả:

```bash
tree results
head results/Task_1-1.csv
head results/Task_1-2.csv
ls -lh results/Task_2-1.parquet
ls -lh results/Task_2-2.parquet
cat logs/task_2-2_stats.log
```

## 7. Chạy benchmark

Cấp quyền thực thi:

```bash
chmod +x benchmark_all.sh
```

Hoặc cấp quyền cho cả hai script:

```bash
chmod +x build_and_run_all.sh benchmark_all.sh
```

Chạy benchmark:

```bash
./benchmark_all.sh
```

Script benchmark sẽ chạy cả 4 task:

```text
Task_1-1
Task_1-2
Task_2-1
Task_2-2
```

Mỗi task gồm:

- 1 lần warm-up
- 5 lần đo chính thức

Warm-up được lưu riêng trong JSON, nhưng không được tính vào thống kê chính thức.

Các file benchmark được tạo:

```text
logs/Task_1-1.json
logs/Task_1-2.json
logs/Task_2-1.json
logs/Task_2-2.json
```

Kiểm tra benchmark:

```bash
tree logs
cat logs/Task_1-1.json
cat logs/Task_1-2.json
cat logs/Task_2-1.json
cat logs/Task_2-2.json
```

Benchmark chỉ đo thời gian chạy job chính. Các bước sau không được tính vào thời gian benchmark:

- biên dịch Scala
- đóng gói JAR
- chuẩn bị input HDFS
- copy output về local
- xử lý hậu kỳ local

## 8. Chạy riêng từng task nếu cần debug

### 8.1. Task 1-1

```bash
cd ~/AI_Project/BigData-Lab03-AdvancedMR-Spark/src/Task_1-1

export SCALA_LIBRARY_JAR="$SCALA_HOME/lib/scala-library.jar"
export HADOOP_CLASSPATH="$(hadoop classpath):$SCALA_LIBRARY_JAR"

mkdir -p classes
rm -rf classes/*
rm -f SlidingWindowJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-1.scala

cd classes
jar xf "$SCALA_LIBRARY_JAR"
cd ..

jar -cvf SlidingWindowJob.jar -C classes .

hadoop fs -rm -r -f /lab03/output/task1-1

hadoop jar SlidingWindowJob.jar lab03.SlidingWindowJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-1

mkdir -p ../../results
rm -f ../../results/Task_1-1.csv

hadoop fs -getmerge /lab03/output/task1-1 ../../results/Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' ../../results/Task_1-1.csv

head ../../results/Task_1-1.csv

cd ../..
```

### 8.2. Task 1-2

```bash
cd ~/AI_Project/BigData-Lab03-AdvancedMR-Spark/src/Task_1-2

export SCALA_LIBRARY_JAR="$SCALA_HOME/lib/scala-library.jar"
export HADOOP_CLASSPATH="$(hadoop classpath):$SCALA_LIBRARY_JAR"

mkdir -p classes
rm -rf classes/*
rm -f MedianVarietyJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-2.scala

cd classes
jar xf "$SCALA_LIBRARY_JAR"
cd ..

jar -cvf MedianVarietyJob.jar -C classes .

hadoop fs -rm -r -f /lab03/output/task1-2-temp
hadoop fs -rm -r -f /lab03/output/task1-2

hadoop jar MedianVarietyJob.jar lab03.MedianVarietyJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-2-temp \
  /lab03/output/task1-2

mkdir -p ../../results
rm -f ../../results/Task_1-2.csv

hadoop fs -getmerge /lab03/output/task1-2 ../../results/Task_1-2.csv
sed -i '2,${/^Month,State/d}' ../../results/Task_1-2.csv

head ../../results/Task_1-2.csv

cd ../..
```

### 8.3. Task 2-1

```bash
cd ~/AI_Project/BigData-Lab03-AdvancedMR-Spark/src/Task_2-1

export SCALA_LIBRARY_JAR="$SCALA_HOME/lib/scala-library.jar"
export HADOOP_CLASSPATH="$(hadoop classpath):$SCALA_LIBRARY_JAR"
export SPARK_CLASSPATH="$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')"
export HDFS_URI="$(hdfs getconf -confKey fs.defaultFS)"
export SPARK_INPUT_PATH="${HDFS_URI}/lab03/input/Amazon_Sale_Report.csv"
export SPARK_TASK21_OUTPUT_PATH="${HDFS_URI}/lab03/output/Task_2-1.parquet"

mkdir -p classes
rm -rf classes/*
rm -f SparkTask21.jar

scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-1.scala
jar -cvf SparkTask21.jar -C classes lab3

hadoop fs -rm -r -f /lab03/output/Task_2-1.parquet
hadoop fs -rm -r -f /lab03/output/Task_2-1.parquet_staging

spark-submit \
  --class lab3.task21.SparkTask21 \
  --master local[*] \
  --conf "spark.ui.showConsoleProgress=false" \
  SparkTask21.jar \
  "$SPARK_INPUT_PATH" \
  "$SPARK_TASK21_OUTPUT_PATH"

mkdir -p ../../results
rm -rf ../../results/Task_2-1.parquet

hadoop fs -get /lab03/output/Task_2-1.parquet ../../results/Task_2-1.parquet

cd ../..
```

### 8.4. Task 2-2

```bash
cd ~/AI_Project/BigData-Lab03-AdvancedMR-Spark/src/Task_2-2

export SCALA_LIBRARY_JAR="$SCALA_HOME/lib/scala-library.jar"
export HADOOP_CLASSPATH="$(hadoop classpath):$SCALA_LIBRARY_JAR"
export SPARK_CLASSPATH="$(find "$SPARK_HOME/jars" -name "*.jar" | tr '\n' ':')"
export HDFS_URI="$(hdfs getconf -confKey fs.defaultFS)"
export SPARK_INPUT_PATH="${HDFS_URI}/lab03/input/Amazon_Sale_Report.csv"
export SPARK_TASK22_OUTPUT_PATH="${HDFS_URI}/lab03/output/Task_2-2.parquet"

mkdir -p classes
rm -rf classes/*
rm -f SparkTask22.jar

scalac -classpath "$HADOOP_CLASSPATH:$SPARK_CLASSPATH" -d classes Task_2-2.scala
jar -cvf SparkTask22.jar -C classes .

hadoop fs -rm -r -f /lab03/output/Task_2-2.parquet
hadoop fs -rm -r -f /lab03/output/Task_2-2.parquet_staging

mkdir -p ../../logs

spark-submit \
  --class Task22 \
  --master local[*] \
  --conf "spark.ui.showConsoleProgress=false" \
  SparkTask22.jar \
  "$SPARK_INPUT_PATH" \
  "$SPARK_TASK22_OUTPUT_PATH" \
  2>&1 | tee ../../logs/task_2-2_stats.log

mkdir -p ../../results
rm -rf ../../results/Task_2-2.parquet

hadoop fs -get /lab03/output/Task_2-2.parquet ../../results/Task_2-2.parquet

cd ../..
```

## 9. Kiểm tra định dạng output

### 9.1. Task 1-1

```text
State,TargetDate,MostBoughtSize,Count
```

Kiểm tra:

```bash
head results/Task_1-1.csv
```

### 9.2. Task 1-2

```text
Month,State,MedianVariety
```

Kiểm tra:

```bash
head results/Task_1-2.csv
```

### 9.3. Task 2-1

Output là một file Parquet duy nhất:

```text
results/Task_2-1.parquet
```

### 9.4. Task 2-2

Output là một file Parquet duy nhất:

```text
results/Task_2-2.parquet
```

File thống kê và log phân tích:

```text
logs/task_2-2_stats.log
```

## 10. Dọn dẹp file build tạm

Xóa các file build local:

```bash
rm -rf src/Task_1-1/classes
rm -rf src/Task_1-2/classes
rm -rf src/Task_2-1/classes
rm -rf src/Task_2-2/classes

rm -f src/Task_1-1/SlidingWindowJob.jar
rm -f src/Task_1-2/MedianVarietyJob.jar
rm -f src/Task_2-1/SparkTask21.jar
rm -f src/Task_2-2/SparkTask22.jar
```

Xóa file checksum `.crc` nếu có:

```bash
find . -name "*.crc" -type f -delete
```

Xóa Spark temp file nếu cần:

```bash
rm -rf /tmp/spark-*
rm -rf /tmp/blockmgr-*
```

Xóa input và output HDFS cũ:

```bash
hadoop fs -rm -r -f /lab03/input
hadoop fs -rm -r -f /lab03/output
```

## 11. Clean toàn bộ trước khi chạy lại hai script

Chạy tại project root:

```bash
cd ~/AI_Project/BigData-Lab03-AdvancedMR-Spark

rm -rf src/Task_1-1/classes src/Task_1-2/classes src/Task_2-1/classes src/Task_2-2/classes
rm -f src/Task_1-1/SlidingWindowJob.jar
rm -f src/Task_1-2/MedianVarietyJob.jar
rm -f src/Task_2-1/SparkTask21.jar
rm -f src/Task_2-2/SparkTask22.jar

rm -rf results logs
mkdir -p results logs

find . -name "*.crc" -type f -delete
rm -rf /tmp/spark-* /tmp/blockmgr-*

hadoop fs -rm -r -f /lab03/input
hadoop fs -rm -r -f /lab03/output
```

Kiểm tra môi trường:

```bash
java -version
scala -version
scalac -version
spark-submit --version 2>&1 | grep -i scala
hdfs getconf -confKey fs.defaultFS
jps
```

Cấp quyền và chạy:

```bash
chmod +x build_and_run_all.sh benchmark_all.sh
./build_and_run_all.sh
./benchmark_all.sh
```

Sau khi chạy xong, kiểm tra:

```bash
tree results
tree logs
head results/Task_1-1.csv
head results/Task_1-2.csv
ls -lh results/Task_2-1.parquet
ls -lh results/Task_2-2.parquet
cat logs/Task_1-1.json
cat logs/Task_1-2.json
cat logs/Task_2-1.json
cat logs/Task_2-2.json
```

## 12. Tắt Hadoop

```bash
stop-yarn.sh
stop-dfs.sh
```

Kiểm tra:

```bash
jps
```

Nếu chỉ còn `Jps` hoặc không còn các tiến trình Hadoop chính, Hadoop đã được tắt.
