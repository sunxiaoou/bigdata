#! /bin/bash

cd ../../
path=xo/protobuf
for i in `echo $path/*.proto`
do
  protoc -I $path/  --java_out . "$i"
	# a=${i%\.*}Proto.java
	a=`sed -n 's/option java_outer_classname = "\([^"]*\)";/\1/p' "$i"`
	a=$path/"$a".java
	# mv "$a" "$a".orig
  # sed "s/com.google.protobuf/org.apache.hbase.thirdparty.com.google.protobuf/g" "$a".orig > "$a"
done
