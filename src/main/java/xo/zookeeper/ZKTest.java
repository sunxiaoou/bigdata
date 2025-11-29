package xo.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKTest {
    private static final Logger LOG = LoggerFactory.getLogger(ZKTest.class);

    public static void main(String[] args) throws Exception {
//        System.setProperty("java.net.preferIPv4Stack", "true");
//        System.setProperty("zookeeper.sasl.client", "false");
        String hostPort = "ubuntu:2181";
        LOG.info("Connecting to {}", hostPort);

        ZooKeeper zk = new ZooKeeper(hostPort, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("Watcher event: " + event);
            }
        });

        Thread.sleep(5000); // 等会，看会不会打印 SyncConnected 之类的事件

        try {
            byte[] data = zk.getData("/hbase/hbaseid", false, null);
            LOG.info("/hbase/hbaseid = {}", new String(data, "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        zk.close();
    }
}