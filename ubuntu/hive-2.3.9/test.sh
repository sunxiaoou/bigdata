#!/bin/sh

# nohup bin/hive --service metastore &
# nohup bin/hive --service hiveserver2 &

bin/hive <<-!end
    select * from u_data limit 10;
    select count(*) from u_data;
!end

