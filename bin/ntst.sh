#!/bin/bash

echo ${@: -1}
if [ $# -gt 0 ]; then
	if [ "xlisten" = x"${@: -1}" ]; then
		netstat -lnpt | grep -i TCP | grep `jps | grep -w $1 | awk '{print $1}'` | grep "LISTEN"
	else
		netstat -lnpt  | grep -i TCP | grep `jps | grep -w $1 | awk '{print $1}'`
	fi
else
	netstat -lnpt | grep -i TCP
fi
