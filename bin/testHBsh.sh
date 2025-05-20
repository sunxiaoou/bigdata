#!/bin/bash

# 创建HBase表，处理已存在的情况
hbase shell <<EOF
disable 'manga:export' if exists 'manga:export'
drop 'manga:export' if exists 'manga:export'
create 'manga:export', {NAME => 'cf', VERSIONS => 1}
exit
EOF

# 导入数据
hbase shell <<EOF
put 'manga:export', '12', 'cf:name', '米纳米王国'
put 'manga:export', '23', 'cf:name', '阿尔法帝国'
put 'manga:export', '25', 'cf:name', '理陀儿王国'
put 'manga:export', '27', 'cf:name', '费沙自治领'
EOF