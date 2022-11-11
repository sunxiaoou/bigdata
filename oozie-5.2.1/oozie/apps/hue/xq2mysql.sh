#!/bin/sh

cd ~/learn/py/finance
log=${0%.*}.log
./xueqiu.py $1 > $log 2>&1
echo rows_num=`head -1 $log`
