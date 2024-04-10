package xo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class ZkConnect {
    private static final Logger LOG = LoggerFactory.getLogger(ZkConnect.class);

    private final ZooKeeper zk;

    //host should be 127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002
    public ZkConnect(String connectString) throws IOException, InterruptedException {
        CountDownLatch connSignal = new CountDownLatch(1);
        zk = new ZooKeeper(connectString, 90000, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == KeeperState.SyncConnected) {
                    connSignal.countDown();
                }
            }
        });
        connSignal.await();
    }

    public ZkConnect(String host, int port) throws IOException, InterruptedException {
        this(String.format("%s:%d", host, port));
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public String createNode(String path, byte[] data, boolean isEphemeral) throws KeeperException, InterruptedException {
        String[] parts = path.replaceAll("^/", "").split("/");
        int len = parts.length;
        String partialPath = "";
        String node = null;
        for (int i = 0; i < len; i ++) {
            partialPath += "/" + parts[i];
            if (zk.exists(partialPath, false) == null) {
                byte[] bytes = null;
                CreateMode mode = CreateMode.PERSISTENT;
                if (i == len - 1) {
                    bytes = data;
                    if (isEphemeral) {
                        mode = CreateMode.EPHEMERAL;
                    }
                }
                node = zk.create(partialPath, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
                LOG.info("Created node: " + partialPath);
            }
        }
        return node;
    }

    public void updateNode(String path, byte[] data) throws Exception
    {
        zk.setData(path, data, zk.exists(path, true).getVersion());
    }

    public void deleteNode(String path) throws Exception
    {
        zk.delete(path,  zk.exists(path, true).getVersion());
    }

    private List<String> getChildren(String path) throws KeeperException, InterruptedException {
        return zk.getChildren(path, true);
    }

    private byte[] getData(String path) throws KeeperException, InterruptedException {
        return zk.getData(path, true, zk.exists(path, true));
    }

    public static void test(ZkConnect conn) throws Exception {
        String newNode = "/manga/" + System.currentTimeMillis();
        conn.createNode(newNode, new Date().toString().getBytes(), false);
        List<String> zNodes = conn.getChildren("/");
        for (String zNode: zNodes) {
            LOG.info("ChildrenNode " + zNode);
        }
        byte[] data = conn.getData(newNode);
        LOG.info("GetData before setting");
        for (byte dataPoint: data) {
            System.out.print((char)dataPoint);
        }
        System.out.print("\n");
        conn.updateNode(newNode, "Modified data".getBytes());
        data = conn.getData(newNode);
        LOG.info("GetData after setting");
        for (byte dataPoint: data) {
            System.out.print((char)dataPoint);
        }
        conn.deleteNode(newNode);
    }

    public static void main(String[] args) throws Exception {
//        ZkConnect conn = new ZkConnect("172.20.77.196,172.20.77.197,172.20.77.198");
        ZkConnect conn = new ZkConnect("localhost", 2181);
        test(conn);
        conn.close();
    }
}

