#!/bin/sh

if [ $# -lt 1 ]
then
    echo "Usage: `basename $0` host [host ...]"
    exit 1
fi

hosts=$1
shift

while [ $# -gt 0 ]
do
    hosts="$hosts $1"
    shift
done

user=`whoami`
for host in $hosts
do
    echo ============= $host =============
    ssh $user@$host /opt/jdk/bin/jps | grep -vw "Jps"
done
