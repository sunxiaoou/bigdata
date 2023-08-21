package xo.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class Server {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private static final String groupName = "/servers";
    private final String hostName;
    private final ZooKeeper zk;

    public Server(String hostName) throws Exception {
        this.hostName = hostName;
        String connectString = "localhost:2181";
        int sessionTimeout = 3000;
        CountDownLatch connSignal = new CountDownLatch(0);
        this.zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connSignal.countDown();
                }
            }
        });
    }

    public void register() throws Exception {
        synchronized (this) {
            if (zk.exists(groupName, false) == null) {
                zk.create(groupName, "server list".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
        String address = zk.create(groupName + "/server", hostName.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        LOG.info("Server is starting, reg address：" + address);
    }

    public void unregister() throws Exception {
        zk.close();
    }

    public static void main(String[] args) throws Exception {
        Server server1 = new Server(" server1");
        Server server2 = new Server(" server2");

        Thread thread1 = new Thread(() -> {
            try {
                server1.register();
                Thread.sleep(10000); // Keep server1 running for 10 seconds
                server1.unregister();
                LOG.info("server1 shutdown ...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                server2.register();
                Thread.sleep(5000); // Keep server2 running for 5 seconds
                server2.unregister();
                LOG.info("server2 shutdown ...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Start both threads
        thread1.start();
        thread2.start();

        // Wait for both threads to complete
        thread1.join();
        thread2.join();

        LOG.info("Both servers have completed their lifecycle.");
    }
}
