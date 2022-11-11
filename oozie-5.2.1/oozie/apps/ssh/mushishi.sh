#!/bin/sh

rm /tmp/mushishi_*.txt

echo "$1 $JAVA_HOME" > /tmp/mushishi_$$.txt
# env >> /tmp/mushishi_$$.txt

echo "Mushishi=$1"
