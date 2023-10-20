#!/bin/sh

date=`date +"%Y-%m-%d"`
LOGS=" \
	iahelper_$date.log \
	iawork_$date.log \
	iaback_$date.log \
	iatrack_$date.log \
	iatrack/iatrack-all.log \
	iatrack/iatrack-nohup.out \
	iatrack/iatrack-error.log \
	dbdump/dbdump_all.log \
	dbdump/dbdump_error.log \
	dbdump/dbdump_ipc.log \
	"
tmp=~/tmp/$$
mkdir $tmp
echo $LOGS
cd /var/i2data/log
for i in $LOGS
do
	echo "cp $i $tmp"
	cp $i $tmp
	> $i
done

exit

> iahelper_$date.log
> iawork_$date.log
> iaback_$date.log
> iatrack/iatrack-all.log
> iatrack/iatrack-error.log
> dbdump/dbdump_all.log
> dbdump/dbdump_error.log
> dbdump/dbdump_ipc.log
