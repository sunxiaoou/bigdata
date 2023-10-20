#!/bin/sh

if [ $# -lt 2 ]
then
    echo "Usage: `basename $0` op host [host ...]"
    exit 1
fi

op=$1
shift

hosts=$1
shift

while [ $# -gt 0 ]
do
    hosts="$hosts $1"
    shift
done

case $op in
"start")
    for host in $hosts
    do
        echo "============= zk start on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh start
    done
    ;;
"stop")
    for host in $hosts
    do
        echo "============= zk stop on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh stop
    done
    ;;
"status")
    for host in $hosts
    do
        echo "============= zk status on $host ============="
        ssh $host $ZOOKEEPER_HOME/bin/zkServer.sh status
    done
    ;;
*)
    echo "Usage: $(basename $0) start|stop|status"
    exit 1
    ;;
esac
