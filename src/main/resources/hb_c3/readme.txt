$ kinit -kt hb_c3/hadoop.keytab yarn/centos3@EXAMPLE.COM

$ . ./setenv.sh

$ hdfs dfs -cat /hbase/hbase.id
PBUF
$7df371d6-b146-4fd7-a2d2-261cc3e0905d

$ hbase shell
Acquire TGT from Cache
Principal is yarn/centos3@EXAMPLE.COM
Commit Succeeded
> org.apache.hadoop.hbase.client.ConnectionFactory.createConnection.getClusterId
=> "7df371d6-b146-4fd7-a2d2-261cc3e0905d"
> org.apache.hadoop.security.UserGroupInformation.getCurrentUser.toString
=> yarn/centos3@EXAMPLE.COM (auth:KERBEROS)
> org.apache.hadoop.hbase.HBaseConfiguration.create.get("hbase.rootdir")
=> "hdfs://centos3:8020/hbase"

$ java -cp ".:lib/*:target/bigdata-1.0-SNAPSHOT.jar" xo.hbase.Fruit -p hbase/centos3@EXAMPLE.COM -k hb_c3/hadoop.keytab -h hb_c3 -a scan

