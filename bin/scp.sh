#!/bin/sh

if [ $# -lt 1 ]
then
    echo "Usage: $0 file [file ...]"
    # sed.sh hadoop1 hadoop2 core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 
    exit 1
fi

files=$1
shift

while [ $# -gt 0 ]
do
    files="$files $1"
    shift
done
    
echo $files
user=`whoami`
hosts="hadoop104 hadoop105"
for $host in $hosts
do
    echo scp $files $user@$host:$PWD 
done

