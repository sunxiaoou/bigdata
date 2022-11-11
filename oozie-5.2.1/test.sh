#!/bin/sh

# bin/oozied.sh start

# hdfs dfs -put oozie
hdfs dfs -rm -f -r oozie/data/wordcount/output

# export OOZIE_DEBUG=1
export OOZIE_URL=http://localhost:11000/oozie

echo "bin/oozie job -config oozie/apps/wordcount/job.properties -run"
bin/oozie job -config oozie/apps/wordcount/job.properties -run > /tmp/job_$$
job=`cat /tmp/job_$$ | tail -1 | awk '{print $NF}'`
echo "sleep ...\n"
sleep 20

echo "bin/oozie job -info $job"
echo
bin/oozie job -info $job

