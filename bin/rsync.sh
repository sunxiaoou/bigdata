#!/bin/sh

if [ $# -lt 2 ]
then
    echo "Usage: `basename $0` path host [host ...]"
    exit 1
fi

child=`basename $1`
parent=`cd -P $(dirname $1); pwd`
shift

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
    echo rsync -rvl $parent/$child $user@$host:$parent 
    rsync -rvl $parent/$child $user@$host:$parent 
done
