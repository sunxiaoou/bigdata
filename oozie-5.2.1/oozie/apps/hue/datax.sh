#!/bin/sh

cd ~/work/dataX/Dist/bin
rm ${0%.*}.log
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_321.jdk/Contents/Home
PATH=$JAVA_HOME/bin:.:$PATH datax.py $1 > ${0%.*}.log 2>&1
