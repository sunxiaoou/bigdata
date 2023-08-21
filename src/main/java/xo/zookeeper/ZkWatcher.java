package xo.zookeeper;
import org.apache.zookeeper.*;

public class ZkWatcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 30000;

    public static void main(String[] args) throws Exception {
        ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // Watcher callback logic
                System.out.println("Received event: " + event.getType());
            }
        });

        String znodePath = "/myznode";

        // Create a znode and set a watcher on it
        zooKeeper.create(znodePath, "Hello, ZooKeeper!".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // Get the data of the znode and set a watcher on it
        byte[] data = zooKeeper.getData(znodePath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("Data changed event: " + event.getType());
                try {
                    // Set the watcher again after processing the event
                    if (zooKeeper.exists(znodePath, true) != null) {
                        zooKeeper.getData(znodePath, this, null);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, null);

        System.out.println("Initial data: " + new String(data));

        // Modify the data of the znode
        zooKeeper.setData(znodePath, "Modified data".getBytes(), -1);

        // Close the ZooKeeper connection
        zooKeeper.delete(znodePath, zooKeeper.exists(znodePath, true).getVersion());
        zooKeeper.close();
    }
}
