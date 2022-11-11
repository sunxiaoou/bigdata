#!/bin/sh

usage() {
	echo "Usage: $0 standalone"
	echo "       $0 start [yarn]"
	echo "       $0 stop [yarn]"
	echo "       $0 pseudo_dist"
}

standalone() {
	cd $HADOOP_HOME/etc/hadoop
	ln -sf core-site.xml.orig core-site.xml
	ln -sf hdfs-site.xml.orig hdfs-site.xml
	ln -sf yarn-site.xml.orig yarn-site.xml
	rm -f mapred-site.xml
	cd $HADOOP_HOME

	mkdir -p input
	echo test apache hadoop hadoop sqoop hue mapreduce sqoop oozie http > input/in.txt
	rm -rf output
	hadoop jar $jarfile wordcount input output
	cat output/*
}

start() {
	cd $HADOOP_HOME/etc/hadoop
	ln -sf core-site.xml.pseudo core-site.xml
	ln -sf hdfs-site.xml.pseudo hdfs-site.xml
	if [ $# -eq 0 ]; then
		ln -sf yarn-site.xml.orig yarn-site.xml
		rm -f mapred-site.xml
	else
		ln -sf yarn-site.xml.pseudo yarn-site.xml
		ln -sf mapred-site.xml.pseudo mapred-site.xml
	fi

	cd $HADOOP_HOME
	# hdfs namenode -format
	if [ "x`jps | grep -w "NameNode"`" = "x" ]; then
		start-dfs.sh
	fi
	# hdfs dfsadmin -safeoper leave
	if [ $# -eq 1 -a "x`jps | grep -w "NodeManager"`" = "x" ]; then
		start-yarn.sh
		mr-jobhistory-daemon.sh start historyserver
	fi
}

stop() {
	if [ $# -eq 1 ]; then
		mr-jobhistory-daemon.sh stop historyserver
		stop-yarn.sh
	fi
	stop-dfs.sh
}

pseudo_dist() {
	hdfs dfs -rm -f -r input
	hdfs dfs -mkdir input
	mkdir -p input
	echo test apache hadoop hadoop sqoop hue mapreduce sqoop oozie http > input/in.txt
	hdfs dfs -put $HADOOP_HOME/input/in.txt input
	hdfs dfs -rm -f -r output
	hadoop jar $jarfile wordcount input output
	hdfs dfs -cat output/*
}

oper=$1
yarn=$2
jarfile=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.0.jar

if [ \( x$oper = "xstandalone" -o x$oper = "xstart" -o x$oper = "xstop" -o x$oper = "xpseudo_dist" \) \
		-a \( x$yarn = "x" -o x$yarn = "xyarn" \) ]; then
	echo "$oper $yarn"
	$oper $yarn
else
	usage
	exit 1
fi
