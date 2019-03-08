Could not start ZK at requested port of 2181.  ZK was started at port: 2182.  Aborting as clients (e.g. shell) will not be 

启动hbase的时候，可以设置 export HBASE_MANAGES_ZK=true，让hbase使用自带的zk。

当时，当启动的时候报错如下：

starting master, logging to /home/wde/hbase/hbase/bin/../logs/hbase-wde-master-ict003.out
Could not start ZK at requested port of 2181. ZK was started at port: 2182. Aborting as clients (e.g. shell) will not be able to find this ZK quorum.

 

看样子，应该是默认的2181端口已经被占用了。如果不在hbase-site.xml里面指定zk的端口的话，那么就使用默认的2181端口。一旦2181端口被占用了，就会导致启动失败。

修改hbase-site.xml，添加下面的行：

  <property>
          <name>hbase.zookeeper.property.clientPort</name>
          <value>2182</value>                                                                                                                                             
  </property> 

 

然后就能正常启动hbase了。


错误：ConnectionFactory hbase java.io.IOException: No FileSystem for scheme: hdfs
hadoop依赖包没有配好，配完整hadoop的依赖包

错误：org.apache.hadoop.hbase.client.RpcRetryingCaller-callWithRetries [2019-03-06 20:15:48] - Call exception, tries=10, retries=35, started=45567 ms ago, cancelled=false, msg=
错误： INFO | org.apache.hadoop.hbase.client.RpcRetryingCaller-callWithRetries [2019-03-08 14:05:18] - Call exception, tries=10, retries=35, started=45663 ms ago, cancelled=false, msg=row 'my_test_table,kr001,99999999999999' on table 'hbase:meta' at region=hbase:meta,,1.1588230740, hostname=localhost,16020,1552023602463, seqNum=0

原因：hbase的master，regionserver的hostname都被映射成localhost了，改回来。
解决（网上没有此种解决办法）：
先看下 /etc/hosts 以及 /etc/sysconfig/network 中域名有没有错， window中有没有对hbase服务器的域名做ip映射。
如果仍没有解决问题，则hbase/conf/hbase-site.xml 添加配置：
<property>
                <name>hbase.master.hostname</name>
                <value>node1</value>
        </property>
        <property>
                <name>hbase.regionserver.hostname</name>
                <value>node1</value>
        </property>

重启hbase。