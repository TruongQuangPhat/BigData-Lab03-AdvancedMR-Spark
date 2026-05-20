# LAB 03 - ADVANCED MAPREDUCE PROBLEMS

![Hadoop](https://img.shields.io/badge/Hadoop-3.x-blue?logo=apachehadoop&logoColor=white) ![Spark](https://img.shields.io/badge/Spark-3.x-orange?logo=apachespark&logoColor=white) ![Scala](https://img.shields.io/badge/Scala-2.12.18-red?logo=scala&logoColor=white) ![Java](https://img.shields.io/badge/Java-8-007396?logo=java&logoColor=white)

## Hướng dẫn Build, Chạy và Benchmark

README này hướng dẫn cách chuẩn bị môi trường, chạy các bài hiện có của Lab 03 và đo thời gian thực thi phục vụ phần benchmark trong báo cáo.

Phần MapReduce hiện tại:

- `Task_1-1`: Sliding Window
- `Task_1-2`: Median Variety

Phần Spark:

- `Task_2-1`: Cancelled Standard Order Qualification Percentage
- `Task_2-2`: Population Standard Deviation with Dynamic Percentiles

## YÊU CẦU HỆ THỐNG

- Hadoop 3.x ở chế độ pseudo-distributed mode
- Apache Spark 3.5.x build với Scala 2.12
- Scala 2.12.18
- Java 8
- Python 3 để xử lý thống kê benchmark và ghi log JSON
- Môi trường Linux hoặc WSL

Kiểm tra môi trường:

```bash
java -version
scala -version
scalac -version
spark-submit --version 2>&1 | grep -i scala
hdfs getconf -confKey fs.defaultFS
```

Kết quả Scala mong đợi:

```text
Scala code runner version 2.12.18
Scala compiler version 2.12.18
Using Scala version 2.12.18
```

Lưu ý quan trọng: version Scala dùng để biên dịch phải tương thích với version Scala của Spark runtime. Trong môi trường đã kiểm thử, Spark sử dụng Scala 2.12.18, vì vậy `scala` và `scalac` cũng cần trỏ tới Scala 2.12.18.

## CẤU TRÚC THƯ MỤC QUAN TRỌNG

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

Trong đó:

- `data/` chứa dữ liệu đầu vào local.
- `src/` chứa mã nguồn Scala.
- `results/` chứa các file kết quả cuối cùng dùng để nộp bài.
- `logs/` chứa log benchmark và log phân tích phục vụ báo cáo.
- `build_and_run_all.sh` dùng để build và chạy toàn bộ các task.
- `benchmark_all.sh` dùng để đo thời gian thực thi của các task.

## CHUẨN BỊ MÔI TRƯỜNG VÀ DỮ LIỆU TRÊN HDFS

Tất cả các lệnh dưới đây được thực thi trong môi trường Linux hoặc WSL.

### 1. Clone repository

Clone repository bằng lệnh:

```bash
git clone https://github.com/TruongQuangPhat/BigData-Lab03-AdvancedMR-Spark.git
```

Di chuyển vào thư mục project:

```bash
cd BigData-Lab03-AdvancedMR-Spark
```

### 2. Khởi động Hadoop

```bash
start-dfs.sh
start-yarn.sh
```

### 3. Kiểm tra các tiến trình Hadoop

```bash
jps
```

Cần bảo đảm các tiến trình chính đã chạy, ví dụ:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

### 4. Tạo thư mục input trên HDFS và tải dữ liệu lên

```bash
hadoop fs -mkdir -p /lab03/input/
hadoop fs -put -f data/Amazon_Sale_Report.csv /lab03/input/
```

Kiểm tra dữ liệu trên HDFS:

```bash
hadoop fs -ls /lab03/input/
```

### 5. Tạo thư mục output local

```bash
mkdir -p results logs
```

## TASK 1-1: SLIDING WINDOW

![Hadoop](https://img.shields.io/badge/Hadoop-3.x-blue?logo=apachehadoop&logoColor=white) ![Scala](https://img.shields.io/badge/Scala-2.12.18-red?logo=scala&logoColor=white)

### Tổng quan

Task 1-1 sử dụng Hadoop MapReduce để thực hiện truy vấn Sliding Window. Với mỗi bang và mỗi ngày mục tiêu, chương trình xác định kích cỡ sản phẩm được mua nhiều nhất trong tối đa 7 ngày trước đó. Kết quả cuối cùng được xuất ra một file CSV duy nhất trong thư mục `results/`.

### Cấu trúc source

- File source: `src/Task_1-1/Task_1-1.scala`
- Package: `lab03`
- Main class: `lab03.SlidingWindowJob`
- Input HDFS: `/lab03/input/Amazon_Sale_Report.csv`
- Output HDFS: `/lab03/output/task1-1`
- Output local cuối cùng: `results/Task_1-1.csv`

### 1. Di chuyển vào thư mục mã nguồn

```bash
cd src/Task_1-1
```

### 2. Thiết lập classpath Hadoop và Scala

Xác định `scala-library.jar` theo compiler `scalac` hiện tại và thiết lập Hadoop classpath:

```bash
export SCALA_LIBRARY_JAR="${SCALA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which scalac)")")")}/lib/scala-library.jar"
export HADOOP_CLASSPATH="$(hadoop classpath):$SCALA_LIBRARY_JAR"
```

Kiểm tra lại đường dẫn Scala runtime:

```bash
echo "$SCALA_LIBRARY_JAR"
ls -l "$SCALA_LIBRARY_JAR"
```

### 3. Biên dịch và đóng gói JAR

Vì task MapReduce được viết bằng Scala nhưng chạy trong Hadoop YARN container, cần đóng gói kèm `scala-library.jar` vào JAR.

```bash
mkdir -p classes
rm -rf classes/*
rm -f SlidingWindowJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-1.scala

cd classes
jar xf "$SCALA_LIBRARY_JAR"
cd ..

jar -cvf SlidingWindowJob.jar -C classes .
```

### 4. Chạy Hadoop MapReduce job

```bash
hadoop fs -rm -r -f /lab03/output/task1-1

hadoop jar SlidingWindowJob.jar lab03.SlidingWindowJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-1
```

### 5. Lấy kết quả từ HDFS về thư mục `results/`

Hadoop MapReduce ghi output ra thư mục HDFS, thường gồm các file `part-r-*`. Vì yêu cầu nộp là một file `.csv` duy nhất, cần dùng `getmerge` để gộp kết quả về local.

```bash
mkdir -p ../../results
rm -f ../../results/Task_1-1.csv

hadoop fs -getmerge /lab03/output/task1-1 ../../results/Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' ../../results/Task_1-1.csv

head ../../results/Task_1-1.csv
```

### 6. Quay lại thư mục gốc

```bash
cd ../..
```

## TASK 1-2: MEDIAN VARIETY

![Hadoop](https://img.shields.io/badge/Hadoop-3.x-blue?logo=apachehadoop&logoColor=white) ![Scala](https://img.shields.io/badge/Scala-2.12.18-red?logo=scala&logoColor=white)

### Tổng quan

Task 1-2 sử dụng Hadoop MapReduce để tính median variety theo từng tháng và từng bang. Chương trình xử lý qua hai MapReduce jobs nối tiếp nhau: job đầu tiên tính số SKU phân biệt của từng style thỏa điều kiện kích cỡ, job thứ hai gom nhóm theo tháng và bang để tính median. Kết quả cuối cùng được xuất ra một file CSV duy nhất trong thư mục `results/`.

### Cấu trúc source

- File source: `src/Task_1-2/Task_1-2.scala`
- Package: `lab03`
- Main class: `lab03.MedianVarietyJob`
- Input HDFS: `/lab03/input/Amazon_Sale_Report.csv`
- Temp output HDFS: `/lab03/output/task1-2-temp`
- Final output HDFS: `/lab03/output/task1-2`
- Output local cuối cùng: `results/Task_1-2.csv`

### 1. Di chuyển vào thư mục mã nguồn

```bash
cd src/Task_1-2
```

### 2. Thiết lập classpath Hadoop và Scala

Xác định `scala-library.jar` theo compiler `scalac` hiện tại và thiết lập Hadoop classpath:

```bash
export SCALA_LIBRARY_JAR="${SCALA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which scalac)")")")}/lib/scala-library.jar"
export HADOOP_CLASSPATH="$(hadoop classpath):$SCALA_LIBRARY_JAR"
```

Kiểm tra lại đường dẫn Scala runtime:

```bash
echo "$SCALA_LIBRARY_JAR"
ls -l "$SCALA_LIBRARY_JAR"
```

### 3. Biên dịch và đóng gói JAR

Vì task MapReduce được viết bằng Scala nhưng chạy trong Hadoop YARN container, cần đóng gói kèm `scala-library.jar` vào JAR.

```bash
mkdir -p classes
rm -rf classes/*
rm -f MedianVarietyJob.jar

scalac -classpath "$HADOOP_CLASSPATH" -d classes Task_1-2.scala

cd classes
jar xf "$SCALA_LIBRARY_JAR"
cd ..

jar -cvf MedianVarietyJob.jar -C classes .
```

### 4. Chạy Hadoop MapReduce job

Task 1-2 cần truyền đủ ba tham số:

1. Input path trên HDFS
2. Temp output path cho Job 1
3. Final output path cho Job 2

```bash
hadoop fs -rm -r -f /lab03/output/task1-2-temp
hadoop fs -rm -r -f /lab03/output/task1-2

hadoop jar MedianVarietyJob.jar lab03.MedianVarietyJob \
  /lab03/input/Amazon_Sale_Report.csv \
  /lab03/output/task1-2-temp \
  /lab03/output/task1-2
```

### 5. Lấy kết quả từ HDFS về thư mục `results/`

```bash
mkdir -p ../../results
rm -f ../../results/Task_1-2.csv

hadoop fs -getmerge /lab03/output/task1-2 ../../results/Task_1-2.csv
sed -i '2,${/^Month,State/d}' ../../results/Task_1-2.csv

head ../../results/Task_1-2.csv
```

### 6. Quay lại thư mục gốc

```bash
cd ../..
```

## TASK 2-1: SPARK STRUCTURED API

![Spark](https://img.shields.io/badge/Spark-3.x-orange?logo=apachespark&logoColor=white) ![Scala](https://img.shields.io/badge/Scala-2.12.18-red?logo=scala&logoColor=white)

### Tổng quan

Task 2-1 sử dụng Spark DataFrame API để tính tỷ lệ các đơn hàng bị hủy thuộc nhóm Standard và thỏa các điều kiện về promotion, amount và state-level average amount. Chương trình chạy bằng Spark Structured API và xuất kết quả cuối cùng ra một file Parquet duy nhất trong thư mục `results/`.

### Cấu trúc source

- File source: `src/Task_2-1/Task_2-1.scala`
- Package: `lab3.task21`
- Main class: `lab3.task21.SparkTask21`
- Input HDFS: `/lab03/input/Amazon_Sale_Report.csv`
- Output HDFS: `/lab03/output/Task_2-1.parquet`
- Output local cuối cùng: `results/Task_2-1.parquet`
- Output format: single Parquet file

### 1. Di chuyển vào thư mục mã nguồn

```bash
cd src/Task_2-1
```

### 2. Cấu hình classpath và biên dịch

```bash
export SCALA_LIBRARY_JAR="${SCALA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which scalac)")")")}/lib/scala-library.jar"
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
```

### 3. Submit job lên Spark

```bash
hadoop fs -rm -r -f /lab03/output/Task_2-1.parquet
hadoop fs -rm -r -f /lab03/output/Task_2-1.parquet_staging

spark-submit \
  --class lab3.task21.SparkTask21 \
  --master local[*] \
  --conf "spark.ui.showConsoleProgress=false" \
  SparkTask21.jar \
  "$SPARK_INPUT_PATH" \
  "$SPARK_TASK21_OUTPUT_PATH"
```

### 4. Lấy kết quả Parquet về máy

```bash
mkdir -p ../../results
rm -rf ../../results/Task_2-1.parquet

hadoop fs -get /lab03/output/Task_2-1.parquet ../../results/Task_2-1.parquet
cd ../..
```

## TASK 2-2: SPARK STRUCTURED API

![Spark](https://img.shields.io/badge/Spark-3.x-orange?logo=apachespark&logoColor=white) ![Scala](https://img.shields.io/badge/Scala-2.12.18-red?logo=scala&logoColor=white)

### Tổng quan

Task 2-2 sử dụng Spark DataFrame API để tính độ lệch chuẩn tổng thể của `Amount` theo từng nhóm SKU-tháng với hai ngưỡng phân vị động P80 và P90. Chương trình triển khai cả phương pháp approximate percentile và exact percentile, xuất kết quả cuối cùng ra một file Parquet duy nhất trong thư mục `results/`, đồng thời lưu log phân tích vào thư mục `logs/`.

### Cấu trúc source

- File source: `src/Task_2-2/Task_2-2.scala`
- Main class: `Task22`
- Input HDFS: `/lab03/input/Amazon_Sale_Report.csv`
- Output HDFS: `/lab03/output/Task_2-2.parquet`
- Output local cuối cùng: `results/Task_2-2.parquet`
- Log phân tích: `logs/task_2-2_stats.log`
- Output format: single Parquet file

### 1. Di chuyển vào thư mục mã nguồn

```bash
cd src/Task_2-2
```

### 2. Cấu hình classpath và biên dịch

```bash
export SCALA_LIBRARY_JAR="${SCALA_HOME:-$(dirname "$(dirname "$(readlink -f "$(which scalac)")")")}/lib/scala-library.jar"
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
```

### 3. Submit job lên Spark và xuất log

```bash
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
```

### 4. Lấy kết quả Parquet về máy

```bash
mkdir -p ../../results
rm -rf ../../results/Task_2-2.parquet

hadoop fs -get /lab03/output/Task_2-2.parquet ../../results/Task_2-2.parquet
cd ../..
```

## KIỂM TRA ĐỊNH DẠNG KẾT QUẢ

Các file kết quả được lưu tại:

```text
results/Task_1-1.csv
results/Task_1-2.csv
results/Task_2-1.parquet
results/Task_2-2.parquet
```

### Task 1-1 Output Format

```csv
State,TargetDate,MostBoughtSize,Count
ANDAMAN & NICOBAR,04-02-22,M,1
ANDHRA PRADESH,04-02-22,M,41
...
```

### Task 1-2 Output Format

```csv
Month,State,MedianVariety
2022-03,ANDHRA PRADESH,1.0
2022-04,MAHARASHTRA,3.5
...
```

### Task 2-1 Output Format

Định dạng xuất ra là một file Parquet duy nhất, gồm 2 cột:

```text
City
Percentage_Qualified_Cancelled_Standard
```

### Task 2-2 Output Format

Định dạng file xuất ra là Parquet dạng wide format, bao gồm 15 cột:

```text
SKU
Month
total_orders
threshold_p80_approx
threshold_p90_approx
orders_p80_approx
orders_p90_approx
stddev_p80_approx
stddev_p90_approx
threshold_p80_exact
threshold_p90_exact
orders_p80_exact
orders_p90_exact
stddev_p80_exact
stddev_p90_exact
```

## QUY TRÌNH TỰ ĐỘNG HÓA: BUILD & RUN ALL

Dự án cung cấp script `build_and_run_all.sh` để tự động hóa quy trình chạy `Task_1-1`, `Task_1-2`, `Task_2-1` và `Task_2-2`.

Script này thực hiện các bước:

1. Chuẩn bị dữ liệu input trên HDFS.
2. Xác định `SPARK_HOME`, HDFS URI và Scala runtime phù hợp.
3. Biên dịch mã nguồn Scala.
4. Đóng gói file JAR.
5. Với các task MapReduce, đóng gói kèm `scala-library.jar` vào JAR để chạy ổn định trong YARN container.
6. Chạy Hadoop MapReduce job cho `Task_1-1`, `Task_1-2` và Spark job cho `Task_2-1`, `Task_2-2`.
7. Trích xuất kết quả cuối cùng từ HDFS về thư mục `results/`.
8. Lưu log phân tích của `Task_2-2` vào thư mục `logs/`.

Các file kết quả sau khi chạy script:

```text
results/Task_1-1.csv
results/Task_1-2.csv
results/Task_2-1.parquet
results/Task_2-2.parquet
logs/task_2-2_stats.log
```

### 1. Cấp quyền thực thi

Chỉ cần thực hiện một lần:

```bash
chmod +x build_and_run_all.sh
```

### 2. Chạy script

```bash
./build_and_run_all.sh
```

### 3. Kiểm tra kết quả

```bash
tree results
head results/Task_1-1.csv
head results/Task_1-2.csv
ls -lh results/Task_2-1.parquet
ls -lh results/Task_2-2.parquet
cat logs/task_2-2_stats.log
```

## QUY TRÌNH BENCHMARK

Dự án cung cấp script `benchmark_all.sh` để đo thời gian thực thi cho các task hiện có.

Ở phiên bản hiện tại, script benchmark hỗ trợ:

- `Task_1-1`: Sliding Window
- `Task_1-2`: Median Variety
- `Task_2-1`: Cancelled Standard Order Qualification Percentage
- `Task_2-2`: Population Standard Deviation with Dynamic Percentiles

Mỗi task được chạy 1 lần warm-up trước, sau đó chạy 5 lần đo chính thức. Lần warm-up chỉ dùng để làm nóng môi trường thực thi và không được đưa vào thống kê benchmark chính thức. Kết quả benchmark được lưu thành các file log JSON riêng biệt:

```text
logs/Task_1-1.json
logs/Task_1-2.json
logs/Task_2-1.json
logs/Task_2-2.json
```

Các file log chứa:

- số lần warm-up;
- thời gian chạy từng lần đo chính thức;
- thời gian trung bình;
- độ lệch chuẩn;
- thời gian nhỏ nhất;
- thời gian lớn nhất;
- thông tin main class và output path tương ứng.

Benchmark chỉ đo thời gian thực thi job chính của 5 lần đo chính thức. Lần warm-up, các bước biên dịch Scala, đóng gói JAR, chuẩn bị input HDFS, copy kết quả về local và xử lý hậu kỳ không được tính vào thống kê benchmark.

### 1. Cấp quyền thực thi

Chỉ cần thực hiện một lần:

```bash
chmod +x benchmark_all.sh
```

### 2. Chạy benchmark

```bash
./benchmark_all.sh
```

### 3. Kiểm tra file log benchmark

```bash
tree logs
cat logs/Task_1-1.json
cat logs/Task_1-2.json
cat logs/Task_2-1.json
cat logs/Task_2-2.json
```
