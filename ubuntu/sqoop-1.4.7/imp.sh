#!/bin/sh

imp_dir=/user/sunxo/sqoop/fruit
bin/sqoop import --connect jdbc:mysql://localhost:3306/manga --username manga --password manga \
    --table fruit --target-dir $imp_dir --num-mappers 1 --delete-target-dir
hdfs dfs -text "$imp_dir/*"
