#!/bin/sh

run() {
    bin/spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 512m \
        --executor-memory 512m \
        --num-executors 1 \
        --class org.apache.spark.examples.SparkPi \
        examples/jars/spark-examples_2.12-3.2.1.jar 10
}


run_py() {
    bin/spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 512m \
        --executor-memory 512m \
        --num-executors 1 \
        examples/src/main/python/pi.py
}

## main ##
# $ sbin/start-history-server.sh

# run
run_py
appid=`grep "APPID" $HADOOP_HOME/logs/yarn*.log | tail -1 | awk 'pirnt $NF'`
appid=${appid#*APPID=}
echo $appid
$HADOOP_HOME/bin/yarn logs -applicationId $appid
