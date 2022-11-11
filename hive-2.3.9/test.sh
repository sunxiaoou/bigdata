#!/bin/sh

strt() {
    nohup bin/hive --service metastore &
    nohup bin/hive --service hiveserver2 &
}

create_udata() {
bin/hive <<-!end
    drop table if exists default.u_data;
    create table default.u_data (
        userid INT,
        movieid INT,
        rating INT,
        unixtime STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;    
!end
}

test_udata() {
bin/hive <<-!end
    select * from default.u_data limit 10;
    select count(*) from u_data;
!end
}

create_price() {
bin/hive <<-!end
    drop table if exists default.instant_price;
    create table default.instant_price (
        code VARCHAR(10),
        name VARCHAR(20),
        ts TIMESTAMP,
        price FLOAT,
        pc FLOAT
    ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;    
!end
}

calc_max_ts() {
bin/hive <<-!end
    select max(ts) from default.instant_price;
!end
}

get_max_ts() {
    a=`calc_max_ts 2>/dev/null | grep "^\d\{4\}-\d\d-\d\d \d\d:\d\d:\d\d$"`
    # a=`date -j -f "%Y-%m-%d %H:%M:%S" "$a" +%y%m%d%H%M%S`
    echo ts=$a
}


## main ##

if [ $# -lt 1 ]
then
    echo "Usage: $0 strt | stop | test_udata | get_max_ts"
    exit 1
fi

# echo $1
$1

