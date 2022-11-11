#!/bin/sh

new_rows() {
cat <<-!end
105,橙子,115.0
106,香蕉,110.0
!end
}

del_rows() {
mysql -umanga -pmanga manga <<-!end
    delete from fruit where fruit_id > 104;
!end
}

list_rows() {
mysql -umanga -pmanga manga <<-!end
    select * from fruit;
!end
}


new_rows > /tmp/$$.txt
del_rows
exp_dir=/user/sunxo/sqoop/export

hdfs dfs -mkdir -p $exp_dir
hdfs dfs -rm "$exp_dir/*"
hdfs dfs -put /tmp/$$.txt $exp_dir
bin/sqoop export --connect jdbc:mysql://localhost:3306/manga --username manga --password manga \
    --table fruit --export-dir $exp_dir --num-mappers 1

list_rows
