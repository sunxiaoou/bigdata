#!/bin/sh

if [ $# -lt 1 ]
then
	echo "Usage: $0 file [file ...]"
	exit 1
fi

while [ $# -gt 0 ]
do
	file=$1
	mv $file $file.orig
	chmod u-w $file.orig
	cp $file.orig $file
	chmod u+w $file
	shift
done
