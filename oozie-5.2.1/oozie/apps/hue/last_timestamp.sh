#!/bin/sh

cd ~/work/hive-2.3.9
ts=`bin/hive -e "select max(ts) from default.instant_price" 2>/dev/null | \
    grep "^\d\{4\}-\d\d-\d\d \d\d:\d\d:\d\d$"`
echo last_ts=$ts

