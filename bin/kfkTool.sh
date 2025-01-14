#!/bin/bash

# 默认的Kafka监听端口
KAFKA_PORT=9092

ACTION=$1
KAFKA_HOSTNAME=$2
TOPIC_NAME=$3

# 检查参数是否提供
if [ -z "$KAFKA_HOSTNAME" ]; then
  echo "Usage: $0 listTopics | showTopic | createTopic | deleteTopic | sendRecord | pollRecords hostname [topicName]"
  exit 1
fi

# 如果没有给定的KAFKA_HOME，尝试在环境变量中查找
if [ -z "$KAFKA_HOME" ]; then
  echo "KAFKA_HOME is not set. Please set the KAFKA_HOME environment variable."
  exit 1
fi

# 定义Kafka相关的命令路径
KAFKA_BIN="$KAFKA_HOME/bin"

# 处理不同的命令
case $ACTION in
  listTopics)
    # 列出所有的topics
    $KAFKA_BIN/kafka-topics.sh --list --bootstrap-server $KAFKA_HOSTNAME:$KAFKA_PORT
    ;;

  showTopic)
    # 显示指定topic的描述信息
    if [ -z "$TOPIC_NAME" ]; then
      echo "Usage: $0 hostname showTopic topicName"
      exit 1
    fi
    $KAFKA_BIN/kafka-topics.sh --describe --topic $TOPIC_NAME --bootstrap-server $KAFKA_HOSTNAME:$KAFKA_PORT
    ;;

  createTopic)
    # 创建新的topic
    if [ -z "$TOPIC_NAME" ]; then
      echo "Usage: $0 hostname createTopic topicName"
      exit 1
    fi
    $KAFKA_BIN/kafka-topics.sh --create --topic $TOPIC_NAME --bootstrap-server $KAFKA_HOSTNAME:$KAFKA_PORT --partitions 1 --replication-factor 1
    echo "Topic $TOPIC_NAME created successfully."
    ;;

  deleteTopic)
    # 删除指定topic
    if [ -z "$TOPIC_NAME" ]; then
      echo "Usage: $0 hostname deleteTopic topicName"
      exit 1
    fi
    $KAFKA_BIN/kafka-topics.sh --delete --topic $TOPIC_NAME --bootstrap-server $KAFKA_HOSTNAME:$KAFKA_PORT
    echo "Topic $TOPIC_NAME deleted successfully."
    ;;

  sendRecord)
    # 向指定topic发送一条记录
    if [ -z "$TOPIC_NAME" ]; then
      echo "Usage: $0 hostname sendRecord topicName"
      exit 1
    fi
    read -p "Enter key: " KEY
    read -p "Enter value: " VALUE
    $KAFKA_BIN/kafka-console-producer.sh --broker-list $KAFKA_HOSTNAME:$KAFKA_PORT --topic $TOPIC_NAME \
        --property "parse.key=true" --property "key.separator=," <<EOF
$KEY,$VALUE
EOF
    echo "Record sent to $TOPIC_NAME with key=$KEY and value=$VALUE."
    ;;

  pollRecords)
    # 从指定topic读取记录
    if [ -z "$TOPIC_NAME" ]; then
      echo "Usage: $0 hostname pollRecords topicName"
      exit 1
    fi
    $KAFKA_BIN/kafka-console-consumer.sh --bootstrap-server $KAFKA_HOSTNAME:$KAFKA_PORT --topic $TOPIC_NAME \
        --from-beginning --property "print.timestamp=true" --property "print.key=true" --max-messages 10 |
      while read -r LINE; do
        OFFSET=$(echo $LINE | awk '{print $1}')
        KEY=$(echo $LINE | awk '{print $2}')
        VALUE=$(echo $LINE | awk '{print $3}')
        echo "Offset: $OFFSET, Key: $KEY, Value: $VALUE"
      done
    ;;

  *)
    echo "Invalid command. Usage: $0 {listTopics|showTopic|createTopic|deleteTopic|sendRecord|pollRecords}"
    exit 1
    ;;
esac