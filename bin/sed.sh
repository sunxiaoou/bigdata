#! /bin/sh

if [ $# -lt 3 ]
then
    echo "Usage: $0 str1 str2 file [file ...]"
    # sed.sh hadoop1 hadoop2 core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml 
    exit 1
fi

str1=$1
shift
str2=$1
shift

while [ $# -gt 0 ]
do
    file=$1
    orig.sh $file
    echo sed -i \"s/$str1/$str2/g\" $file
    sed -i "s/$str1/$str2/g" $file
    shift
done
