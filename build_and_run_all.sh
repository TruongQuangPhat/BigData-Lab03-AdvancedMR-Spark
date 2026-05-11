#!/bin/bash

echo "--- PREPARING HDFS ---"

hadoop fs -mkdir -p /lab2/input/

hadoop fs -test -e /lab2/input/Amazon_Sale_Report.csv || hadoop fs -put data/Amazon_Sale_Report.csv /lab2/input/

export HADOOP_CLASSPATH=$(hadoop classpath):/usr/share/scala/lib/scala-library.jar

echo "--- BUILDING & RUNNING TASK 1-1 ---"

cd src/Task_1-1
mkdir -p classes && rm -rf classes/* && rm -f SlidingWindowJob.jar
scalac -classpath "$HADOOP_CLASSPATH" -d classes SlidingWindowJob.scala
jar -cvf SlidingWindowJob.jar -C classes .
hadoop jar SlidingWindowJob.jar lab2.task11.SlidingWindowJob /lab2/input/Amazon_Sale_Report.csv /lab2/output/task1-1
rm -f Task_1-1.csv
hadoop fs -getmerge /lab2/output/task1-1 Task_1-1.csv
sed -i '2,${/^State,TargetDate/d}' Task_1-1.csv
cd ../..

echo "--- BUILDING & RUNNING TASK 1-2 ---"

cd src/Task_1-2
mkdir -p classes && rm -rf classes/* && rm -f MedianVarietyJob.jar
scalac -classpath "$HADOOP_CLASSPATH" -d classes MedianVarietyJob.scala
jar -cvf MedianVarietyJob.jar -C classes .
hadoop jar MedianVarietyJob.jar lab2.task12.MedianVarietyJob /lab2/input/Amazon_Sale_Report.csv /lab2/output/task1-2-temp /lab2/output/task1-2
rm -f Task_1-2.csv
hadoop fs -getmerge /lab2/output/task1-2 Task_1-2.csv
sed -i '2,${/^Month,State/d}' Task_1-2.csv
cd ../..

echo "HOÀN TẤT BUILD VÀ CHẠY TOÀN BỘ PROJECT!"
